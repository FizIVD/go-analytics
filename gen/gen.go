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

func (pq EventQueue) Len() int { return len(pq) }
func (pq EventQueue) Less(i, j int) bool { return pq[i].Time.Before(pq[j].Time) }
func (pq EventQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *EventQueue) Push(x interface{}) { *pq = append(*pq, x.(*Event)) }
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
    return &ShardedEventGenerator{
        shards:    shards,
        numShards: numShards,
        client:    &http.Client{Timeout: 10 * time.Second},
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
            // если контекст отменён, выходим
            select {
            case <-ctx.Done():
                shard.mu.Unlock()
                return
            default:
            }
            shard.cond.Wait()
        }
        // получаем следующий (минимальный) элемент, но не Pop — чтобы знать время
        ev := heap.Pop(&shard.queue).(*Event)
        shard.mu.Unlock()

        now := time.Now()
        if ev.Time.After(now) {
            // ждём до момента события (не держим mutex)
            wait := ev.Time.Sub(now)
            timer := time.NewTimer(wait)
            select {
            case <-timer.C:
                // время пришло — продолжаем
            case <-ctx.Done():
                if !timer.Stop() {
                    <-timer.C
                }
                return
            }
        }
        seg.processEvent(ctx, ev)
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
    BASE_DAILY_CHURN       = 0.10 // 10% вероятность стать неактивным
    SESSION_LOGOUT_PROB    = 0.10 // 10% вероятность логаута после каждого события
    MAX_DAYS_SINCE_INSTALL = 7

    totalEvents       int64
    totalInstalls     int64
    totalRegistrations int64
    
    // Масштаб времени: 1 минута реального времени = 1 день моделирования
    timeScale = time.Minute
)

// ===================== Отправка событий =====================

func (seg *ShardedEventGenerator) processEvent(ctx context.Context, ev *Event) {
    data, _ := json.Marshal(ev)
    var buf bytes.Buffer
    zw := gzip.NewWriter(&buf)
    _, _ = zw.Write(data)
    zw.Close()

    req, _ := http.NewRequestWithContext(ctx, http.MethodPost, seg.endpoint, &buf)
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
    if ev.Action == "login" && ev.ProfileID != 0 {
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
            u.ProfileID = r.Int63()
            u.RegStep = 0
            // Первая сессия через 5-30 секунд после регистрации
            firstSessionDelay := time.Duration(5+r.Intn(25)) * time.Second
            seg.scheduleSession(u, registrationStart.Add(time.Duration(REG_STEPS)*stepDelay).Add(firstSessionDelay), r)
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
    activeUsers   = make(map[int64]*SimUser) // Используем ProfileID как ключ для зарегистрированных пользователей
    usersMutex    sync.RWMutex
    userGenerators = make(map[int64]*rand.Rand)
    generatorsMutex sync.RWMutex
)

func addActiveUser(u *SimUser) {
    usersMutex.Lock()
    defer usersMutex.Unlock()
    
    if u.ProfileID != 0 {
        // Для зарегистрированных пользователей используем ProfileID
        activeUsers[u.ProfileID] = u
    } else {
        // Для незарегистрированных временно используем хэш DeviceID
        h := fnv.New64a()
        h.Write([]byte(u.DeviceID))
        tempID := int64(h.Sum64())
        activeUsers[tempID] = u
    }
}

func removeActiveUser(profileID int64) {
    usersMutex.Lock()
    delete(activeUsers, profileID)
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

func getUserGenerator(profileID int64) *rand.Rand {
    generatorsMutex.RLock()
    gen, exists := userGenerators[profileID]
    generatorsMutex.RUnlock()
    
    if !exists {
        generatorsMutex.Lock()
        // Создаем новый генератор с уникальным seed для каждого пользователя
        gen = rand.New(rand.NewSource(time.Now().UnixNano() + profileID))
        userGenerators[profileID] = gen
        generatorsMutex.Unlock()
    }
    return gen
}

// ===================== Ежедневная активность =====================

func (seg *ShardedEventGenerator) scheduleDailyActivity(day int, baseTime time.Time) {
    activeUsersList := getActiveUsers()
    log.Printf("Scheduling daily activity for %d active users on day %d", len(activeUsersList), day)
    
    for _, u := range activeUsersList {
        if u.ProfileID == 0 {
            continue // Пропускаем незарегистрированных пользователей
        }
        
        // 10% вероятность стать неактивным навсегда
        r := getUserGenerator(u.ProfileID)
        if r.Float64() < BASE_DAILY_CHURN {
            u.Churned = true
            removeActiveUser(u.ProfileID)
            continue
        }
        
        // Если пользователь активен, планируем сессию в течение "дня"
        if u.Active && !u.Churned {
            // Сессия в случайное время в течение "дня" (1 минуты)
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
    ctx := context.Background()

    // Запускаем воркеров сразу — они будут ждать incoming событий
    seg.Start(ctx)

    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    base := time.Now()

    log.Printf("Starting simulation: %d users per day for %d days", usersPerDay, days)
    log.Printf("Time scale: 1 minute = 1 day")

    // Симуляция по дням — теперь каждый "день" выполняется в реальном времени
    for day := 0; day < days; day++ {
        log.Printf("Processing day %d", day)

        // Создаем новых пользователей в этот "день"
        for i := 0; i < usersPerDay; i++ {
            u := NewSimUser(r, day)
            seg.scheduleUserLifecycle(u, day, base, r)
            addActiveUser(u)
        }

        // Планируем ежедневную активность для существующих пользователей (каждый день)
        if day > 0 {
            seg.scheduleDailyActivity(day, base)
        }

        base = base.Add(timeScale) // следующий "день"
        // Ждём реальное время: 1 minute == 1 simulated day
        time.Sleep(timeScale)
    }

    // после завершения генерации всех дней ждём пока всё прогонится
    seg.Wait()

    // ... печать статистики
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