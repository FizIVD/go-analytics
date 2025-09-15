package main

import (
    "bytes"
    "compress/gzip"
    "container/heap"
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "hash/fnv"
    "log"
    "math/rand"
    "net/http"
    "os"
    "sync"
    "sync/atomic"
    "time"
)

// ===================== Основные структуры =====================

type Event struct {
    DeviceID  string                 `json:"device_id"`
    ProfileID int64                  `json:"profile_id"`
    Action    string                 `json:"action"`
    Extras    map[string]interface{} `json:"extras"`
    Time      time.Time              `json:"-"`
}

type EventQueue []*Event

func (pq EventQueue) Len() int           { return len(pq) }
func (pq EventQueue) Less(i, j int) bool { return pq[i].Time.Before(pq[j].Time) }
func (pq EventQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }
func (pq *EventQueue) Push(x interface{}) {
    *pq = append(*pq, x.(*Event))
}
func (pq *EventQueue) Pop() interface{} {
    old := *pq
    n := len(old)
    item := old[n-1]
    *pq = old[:n-1]
    return item
}

// ===================== Sharded Generator =====================

type ShardedEventGenerator struct {
    shards    []*EventShard
    numShards int
    client    *http.Client
    endpoint  string
}

type EventShard struct {
    id    int
    queue EventQueue
    mu    sync.Mutex
    cond  *sync.Cond
    wg    sync.WaitGroup
}

func NewShardedEventGenerator(numShards int, endpoint string) *ShardedEventGenerator {
    shards := make([]*EventShard, numShards)
    for i := 0; i < numShards; i++ {
        s := &EventShard{id: i, queue: make(EventQueue, 0)}
        heap.Init(&s.queue)
        s.cond = sync.NewCond(&s.mu)
        shards[i] = s
    }
    transport := &http.Transport{
        MaxIdleConns:        100,
        MaxIdleConnsPerHost: 100,
        IdleConnTimeout:     90 * time.Second,
    }
    client := &http.Client{
        Timeout:   15 * time.Second,
        Transport: transport,
    }
    return &ShardedEventGenerator{
        shards:    shards,
        numShards: numShards,
        client:    client,
        endpoint:  endpoint,
    }
}

func (seg *ShardedEventGenerator) getShard(userID string) *EventShard {
    h := fnv.New32a()
    h.Write([]byte(userID))
    return seg.shards[h.Sum32()%uint32(seg.numShards)]
}

func (seg *ShardedEventGenerator) Schedule(ev *Event) {
    shard := seg.getShard(ev.DeviceID)
    shard.mu.Lock()
    heap.Push(&shard.queue, ev)
    shard.cond.Signal()
    shard.mu.Unlock()
}

func (seg *ShardedEventGenerator) Start(ctx context.Context) {
    for _, shard := range seg.shards {
        shard.wg.Add(1)
        go seg.runShard(ctx, shard)
    }
}

func (seg *ShardedEventGenerator) runShard(ctx context.Context, shard *EventShard) {
    defer shard.wg.Done()

    for {
        shard.mu.Lock()
        for shard.queue.Len() == 0 {
            // ждём сигнала или выход при ctx.Done (будет разбудить clearShards через Broadcast)
            if ctx.Err() != nil {
                shard.mu.Unlock()
                return
            }
            shard.cond.Wait()
            if ctx.Err() != nil && shard.queue.Len() == 0 {
                shard.mu.Unlock()
                return
            }
        }
        ev := heap.Pop(&shard.queue).(*Event)
        shard.mu.Unlock()

        now := time.Now()
        if ev.Time.After(now) {
            time.Sleep(ev.Time.Sub(now))
        }
        seg.processEvent(ev)
    }
}

// Очистить очереди (принудительно) и разбудить воркеров
func (seg *ShardedEventGenerator) clearShards() {
    for _, shard := range seg.shards {
        shard.mu.Lock()
        shard.queue = make(EventQueue, 0)
        shard.cond.Broadcast()
        shard.mu.Unlock()
    }
}

func (seg *ShardedEventGenerator) Wait() {
    for _, shard := range seg.shards {
        shard.wg.Wait()
    }
}

// ===================== Пользователь =====================

type SimUser struct {
    DeviceID    string
    ProfileID   int64
    Key         int64 // ключ, который используется в activeUsers map (tempID или profileID)
    RegStep     int
    Active      bool
    InstalledAt int
    Churned     bool
    LoggedOutTD bool
    OS          string
    Type        string
    Model       string
    ScreenX     int
    ScreenY     int
}

// ===================== Глобальные параметры =====================

var (
    OSList  = []string{"android", "ios"}
    Types   = []string{"smartphone", "tablet", "other"}
    Models  = []string{"iphone16", "xiaomi12", "pocoM6pro", "samsungS24", "pixel8"}
    Actions = []string{"view", "click", "purchase"}

    REG_STEPS              = 7
    DROP_PROB_PER_STEP     = 0.10
    BAD_EVENT_PROB         = 0.05
    BASE_DAILY_CHURN       = 0.10
    SESSION_LOGOUT_PROB    = 0.10
    MAX_DAYS_SINCE_INSTALL = 7

    totalEvents        int64
    totalInstalls      int64
    totalRegistrations int64

    timeScale = time.Minute
)

// ===================== Отправка событий =====================

// теперь запросы создаются с собственным контекстом (background + timeout),
// чтобы cancel() управления воркерами не порождал "context canceled" в Do().
func (seg *ShardedEventGenerator) processEvent(ev *Event) {
    data, _ := json.Marshal(ev)
    var buf bytes.Buffer
    zw := gzip.NewWriter(&buf)
    _, _ = zw.Write(data)
    zw.Close()

    reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()

    req, _ := http.NewRequestWithContext(reqCtx, http.MethodPost, seg.endpoint, &buf)
    req.Header.Set("Content-Encoding", "gzip")
    req.Header.Set("Content-Type", "application/json")
    _, err := seg.client.Do(req)
    if err != nil {
        log.Printf("send error: %v", err)
    }

    atomic.AddInt64(&totalEvents, 1)
    if ev.Action == "install" {
        atomic.AddInt64(&totalInstalls, 1)
    }
    if ev.Action == "register" {
        atomic.AddInt64(&totalRegistrations, 1)
    }
}

// ===================== Логика генерации =====================

func enrichExtras(u *SimUser, extras map[string]interface{}) {
    extras["os"] = u.OS
    extras["type"] = u.Type
    extras["model"] = u.Model
    extras["screen_x"] = u.ScreenX
    extras["screen_y"] = u.ScreenY
    extras["active"] = u.Active
    extras["installed_at"] = u.InstalledAt
    extras["churned"] = u.Churned
    extras["logged_out_td"] = u.LoggedOutTD
    if u.ProfileID == 0 && u.RegStep > 0 {
        extras["reg_step"] = u.RegStep
    }
}

func (seg *ShardedEventGenerator) scheduleUserLifecycle(u *SimUser, day int, baseTime time.Time, r *rand.Rand) {
    // install - в течение "дня" (1 минуты реального времени)
    installDelay := time.Duration(r.Intn(int(timeScale.Seconds()))) * time.Second
    seg.Schedule(&Event{
        DeviceID:  u.DeviceID,
        ProfileID: u.ProfileID,
        Action:    "install",
        Extras:    map[string]interface{}{},
        Time:      baseTime.Add(installDelay),
    })

    // регистрация - начинается после установки
    registrationStart := baseTime.Add(installDelay)
    for step := 1; step <= REG_STEPS; step++ {
        u.RegStep = step
        extras := map[string]interface{}{"step": step}
        enrichExtras(u, extras)
        // Шаги регистрации с интервалом 1-5 секунд в масштабе
        stepDelay := time.Duration(1+r.Intn(5)) * time.Second
        seg.Schedule(&Event{
            DeviceID:  u.DeviceID,
            ProfileID: u.ProfileID,
            Action:    "screen_transition",
            Extras:    extras,
            Time:      registrationStart.Add(time.Duration(step-1)*stepDelay),
        })
        if r.Float64() < DROP_PROB_PER_STEP && step < REG_STEPS {
            u.Active = false
            return
        }
        if step == REG_STEPS {
            // назначаем profileID (переходим в registered) — делаем "переключение" в activeUsers map
            newProfile := r.Int63()
            promoteUserToProfile(u, newProfile) // перемещаем запись в activeUsers с tempKey -> profileID
            u.RegStep = 0
            // Создаём явное событие "register" в момент завершения регистрации
            regTime := registrationStart.Add(time.Duration(REG_STEPS) * stepDelay)
            seg.Schedule(&Event{
                DeviceID:  u.DeviceID,
                ProfileID: u.ProfileID,
                Action:    "register",
                Extras:    map[string]interface{}{},
                Time:      regTime,
            })
            // Первая сессия через 5-30 секунд после регистрации
            firstSessionDelay := time.Duration(5+r.Intn(25)) * time.Second
            seg.scheduleSession(u, regTime.Add(firstSessionDelay), r)
        }
    }
}

func (seg *ShardedEventGenerator) scheduleSession(u *SimUser, start time.Time, r *rand.Rand) {
    if !u.Active || u.Churned {
        return
    }

    sessionID := r.Intn(900000) + 100000
    extras := map[string]interface{}{"session_id": sessionID}
    enrichExtras(u, extras)

    seg.Schedule(&Event{
        DeviceID:  u.DeviceID,
        ProfileID: u.ProfileID,
        Action:    "login",
        Extras:    extras,
        Time:      start,
    })

    t := start
    N := 2 + r.Intn(5)

    for i := 0; i < N; i++ {
        // События в сессии с интервалом 2-10 секунд
        eventDelay := time.Duration(2+r.Intn(8)) * time.Second
        t = t.Add(eventDelay)
        act := Actions[r.Intn(len(Actions))]
        extras := map[string]interface{}{"session_id": sessionID}
        enrichExtras(u, extras)

        seg.Schedule(&Event{
            DeviceID:  u.DeviceID,
            ProfileID: u.ProfileID,
            Action:    act,
            Extras:    extras,
            Time:      t,
        })

        // 10% вероятность логаута после каждого события
        if r.Float64() < SESSION_LOGOUT_PROB {
            logoutDelay := time.Duration(1+r.Intn(3)) * time.Second
            extras := map[string]interface{}{"session_id": sessionID}
            enrichExtras(u, extras)
            seg.Schedule(&Event{
                DeviceID:  u.DeviceID,
                ProfileID: u.ProfileID,
                Action:    "logout",
                Extras:    extras,
                Time:      t.Add(logoutDelay),
            })
            return // Завершаем сессию
        }
    }

    // Если не было раннего логаута, завершаем сессию в конце
    logoutDelay := time.Duration(1+r.Intn(3)) * time.Second
    extras = map[string]interface{}{"session_id": sessionID}
    enrichExtras(u, extras)
    seg.Schedule(&Event{
        DeviceID:  u.DeviceID,
        ProfileID: u.ProfileID,
        Action:    "logout",
        Extras:    extras,
        Time:      t.Add(logoutDelay),
    })
}

// ===================== Управление пользователями =====================

// Храним активных пользователей для продолжения симуляции
var (
    activeUsers     = make(map[int64]*SimUser) // ключ = SimUser.Key (tempID или profileID)
    usersMutex      sync.RWMutex
    userGenerators  = make(map[int64]*rand.Rand)
    generatorsMutex sync.RWMutex
)

func addActiveUser(u *SimUser) {
    usersMutex.Lock()
    defer usersMutex.Unlock()

    if u.ProfileID != 0 {
        u.Key = u.ProfileID
        activeUsers[u.Key] = u
        return
    }
    // временный ключ по DeviceID
    h := fnv.New64a()
    h.Write([]byte(u.DeviceID))
    tempID := int64(h.Sum64())
    u.Key = tempID
    activeUsers[u.Key] = u
}

// promoteUserToProfile — при регистрации перемещаем запись в map на ключ = profileID
func promoteUserToProfile(u *SimUser, profileID int64) {
    usersMutex.Lock()
    defer usersMutex.Unlock()
    // удаляем по старому ключ, если есть
    if u.Key != 0 {
        delete(activeUsers, u.Key)
    }
    u.ProfileID = profileID
    u.Key = profileID
    activeUsers[u.Key] = u
}

func removeActiveUserByKey(key int64) {
    usersMutex.Lock()
    delete(activeUsers, key)
    usersMutex.Unlock()
}

func getActiveUsers() []*SimUser {
    usersMutex.RLock()
    defer usersMutex.RUnlock()

    users := make([]*SimUser, 0, len(activeUsers))
    for _, user := range activeUsers {
        users = append(users, user)
    }
    return users
}

// Генератор случайных чисел на пользователя — по ключу (profileID или tempKey)
func getUserGenerator(key int64) *rand.Rand {
    generatorsMutex.RLock()
    gen, exists := userGenerators[key]
    generatorsMutex.RUnlock()

    if !exists {
        generatorsMutex.Lock()
        // уникальный seed для каждого ключа
        gen = rand.New(rand.NewSource(time.Now().UnixNano() + key))
        userGenerators[key] = gen
        generatorsMutex.Unlock()
    }
    return gen
}

// ===================== Ежедневная активность =====================

func (seg *ShardedEventGenerator) scheduleDailyActivity(day int, baseTime time.Time) {
    activeUsersList := getActiveUsers()
    total := len(activeUsersList)
    regCount := 0
    for _, u := range activeUsersList {
        if u.ProfileID != 0 && !u.Churned {
            regCount++
        }
    }
    log.Printf("Scheduling daily activity for %d active users (%d registered) on day %d", total, regCount, day+1)

    for _, u := range activeUsersList {
        // 10% шанс стать неактивным (применяем ко всем)
        r := getUserGenerator(u.Key)
        if r.Float64() < BASE_DAILY_CHURN {
            u.Churned = true
            removeActiveUserByKey(u.Key)
            continue
        }

        // Если пользователь активен и зарегистрирован — планируем сессию
        if u.Active && !u.Churned && u.ProfileID != 0 {
            sessionDelay := time.Duration(r.Intn(int(timeScale.Seconds()))) * time.Second
            seg.scheduleSession(u, baseTime.Add(sessionDelay), r)
        }
    }
}

func randomDeviceID() string {
    return fmt.Sprintf("%08x-%04x-%04x-%04x-%012xR",
        rand.Uint32(),
        rand.Uint32()&0xffff,
        rand.Uint32()&0xffff,
        rand.Uint32()&0xffff,
        rand.Uint64()&0xffffffffffff,
    )
}

func NewSimUser(r *rand.Rand, day int) *SimUser {
    return &SimUser{
        DeviceID:    randomDeviceID(),
        Active:      true,
        InstalledAt: day,
        OS:          OSList[r.Intn(len(OSList))],
        Type:        Types[r.Intn(len(Types))],
        Model:       Models[r.Intn(len(Models))],
        ScreenX:     720 + r.Intn(721),
        ScreenY:     1280 + r.Intn(1281),
    }
}

// ===================== main =====================

func main() {
    var usersPerDay, days, shards int
    flag.IntVar(&usersPerDay, "u", 1000, "Новых пользователей в день")
    flag.IntVar(&days, "d", 1, "Количество дней")
    flag.IntVar(&shards, "s", 8, "Количество шардов")
    flag.Parse()

    endpoint := getenv("API_URL", "http://localhost:8080/event")
    seg := NewShardedEventGenerator(shards, endpoint)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    log.Printf("Starting simulation: %d users per day for %d days", usersPerDay, days)
    log.Printf("Time scale: 1 minute = 1 day")

    base := time.Now()
    seg.Start(ctx)

    for day := 0; day < days; day++ {
        dayStart := base.Add(time.Duration(day) * timeScale)
        log.Printf("Processing day %d", day+1)

        // создаём новых пользователей
        for i := 0; i < usersPerDay; i++ {
            u := NewSimUser(r, day)
            seg.scheduleUserLifecycle(u, day, dayStart, r)
            addActiveUser(u)
        }

        // планируем ежедневную активность (применимо ко всем дням)
        seg.scheduleDailyActivity(day, dayStart)

        // ждём до следующего дня
        nextDay := dayStart.Add(timeScale)
        now := time.Now()
        if now.Before(nextDay) {
            time.Sleep(nextDay.Sub(now))
        }

        // очищаем очереди — сессии обрываются (плановые события на следующий день не должны остаться)
        seg.clearShards()
    }

    // даём воркерам шанс выйти: прозвонить broadcast в clearShards уже сделал; теперь отменяем контекст и ждём
    cancel()
    seg.Wait()

    log.Printf("=== simulation finished ===")
    log.Printf("total events sent: %d", atomic.LoadInt64(&totalEvents))
    log.Printf("total installs: %d", atomic.LoadInt64(&totalInstalls))
    log.Printf("total registrations: %d", atomic.LoadInt64(&totalRegistrations))
    log.Printf("active users remaining: %d", len(getActiveUsers()))
}

func getenv(k, d string) string {
    if v := os.Getenv(k); v != "" {
        return v
    }
    return d
}
