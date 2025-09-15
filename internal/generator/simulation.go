package generator

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// Simulation constants
const (
	REG_STEPS              = 7
	DROP_PROB_PER_STEP     = 0.10
	SESSION_LOGOUT_PROB    = 0.10
	MAX_DAYS_SINCE_INSTALL = 7
	BASE_DAILY_CHURN       = 0.10
)

var (
	// Actions defines the possible user actions in a session.
	Actions = []string{"view", "click", "purchase"}
)

// Simulation manages the overall simulation state and logic.
type Simulation struct {
	scheduler          *ShardedEventGenerator
	timeScale          time.Duration
	totalInstalls      *atomic.Int64
	totalRegistrations *atomic.Int64
}

// NewSimulation creates a new simulation.
func NewSimulation(scheduler *ShardedEventGenerator, timeScale time.Duration, installs, regs *atomic.Int64) *Simulation {
	return &Simulation{
		scheduler:          scheduler,
		timeScale:          timeScale,
		totalInstalls:      installs,
		totalRegistrations: regs,
	}
}

func (s *Simulation) enrichExtras(u *SimUser, extras map[string]interface{}) {
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

// ScheduleUserLifecycle schedules the full lifecycle for a new user.
func (s *Simulation) ScheduleUserLifecycle(u *SimUser, day int, baseTime time.Time, r *rand.Rand) {
	installDelay := time.Duration(r.Intn(int(s.timeScale.Seconds()))) * time.Second
	s.scheduler.Schedule(&Event{
		DeviceID:  u.DeviceID,
		ProfileID: u.ProfileID,
		Action:    "install",
		Extras:    map[string]interface{}{},
		Time:      baseTime.Add(installDelay),
	})
	s.totalInstalls.Add(1)

	registrationStart := baseTime.Add(installDelay)
	for step := 1; step <= REG_STEPS; step++ {
		u.RegStep = step
		extras := map[string]interface{}{"step": step}
		s.enrichExtras(u, extras)
		stepDelay := time.Duration(1+r.Intn(5)) * time.Second
		s.scheduler.Schedule(&Event{
			DeviceID:  u.DeviceID,
			ProfileID: u.ProfileID,
			Action:    "screen_transition",
			Extras:    extras,
			Time:      registrationStart.Add(time.Duration(step-1) * stepDelay),
		})
		if r.Float64() < DROP_PROB_PER_STEP && step < REG_STEPS {
			u.Active = false
			return
		}
		if step == REG_STEPS {
			newProfile := r.Int63()
			PromoteUserToProfile(u, newProfile)
			u.RegStep = 0
			regTime := registrationStart.Add(time.Duration(REG_STEPS) * stepDelay)
			s.scheduler.Schedule(&Event{
				DeviceID:  u.DeviceID,
				ProfileID: u.ProfileID,
				Action:    "register",
				Extras:    map[string]interface{}{},
				Time:      regTime,
			})
			s.totalRegistrations.Add(1)
			firstSessionDelay := time.Duration(5+r.Intn(25)) * time.Second
			s.scheduleSession(u, regTime.Add(firstSessionDelay), r)
		}
	}
}

func (s *Simulation) scheduleSession(u *SimUser, start time.Time, r *rand.Rand) {
	if !u.Active || u.Churned {
		return
	}

	sessionID := r.Intn(900000) + 100000
	extras := map[string]interface{}{"session_id": sessionID}
	s.enrichExtras(u, extras)

	s.scheduler.Schedule(&Event{
		DeviceID:  u.DeviceID,
		ProfileID: u.ProfileID,
		Action:    "login",
		Extras:    extras,
		Time:      start,
	})

	t := start
	N := 2 + r.Intn(5)

	for i := 0; i < N; i++ {
		eventDelay := time.Duration(2+r.Intn(8)) * time.Second
		t = t.Add(eventDelay)
		act := Actions[r.Intn(len(Actions))]
		sessionExtras := map[string]interface{}{"session_id": sessionID}
		s.enrichExtras(u, sessionExtras)

		s.scheduler.Schedule(&Event{
			DeviceID:  u.DeviceID,
			ProfileID: u.ProfileID,
			Action:    act,
			Extras:    sessionExtras,
			Time:      t,
		})

		if r.Float64() < SESSION_LOGOUT_PROB {
			logoutDelay := time.Duration(1+r.Intn(3)) * time.Second
			logoutExtras := map[string]interface{}{"session_id": sessionID}
			s.enrichExtras(u, logoutExtras)
			s.scheduler.Schedule(&Event{
				DeviceID:  u.DeviceID,
				ProfileID: u.ProfileID,
				Action:    "logout",
				Extras:    logoutExtras,
				Time:      t.Add(logoutDelay),
			})
			return
		}
	}

	logoutDelay := time.Duration(1+r.Intn(3)) * time.Second
	finalExtras := map[string]interface{}{"session_id": sessionID}
	s.enrichExtras(u, finalExtras)
	s.scheduler.Schedule(&Event{
		DeviceID:  u.DeviceID,
		ProfileID: u.ProfileID,
		Action:    "logout",
		Extras:    finalExtras,
		Time:      t.Add(logoutDelay),
	})
}

// ScheduleDailyActivity schedules the activity for all active users for a given day.
func (s *Simulation) ScheduleDailyActivity(day int, baseTime time.Time) {
	activeUsersList := GetActiveUsers()
	for _, u := range activeUsersList {
		r := GetUserGenerator(u.Key)
		if r.Float64() < BASE_DAILY_CHURN {
			u.Churned = true
			RemoveActiveUserByKey(u.Key)
			continue
		}

		if u.Active && !u.Churned && u.ProfileID != 0 {
			sessionDelay := time.Duration(r.Intn(int(s.timeScale.Seconds()))) * time.Second
			s.scheduleSession(u, baseTime.Add(sessionDelay), r)
		}
	}
}
