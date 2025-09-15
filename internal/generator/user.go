package generator

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

// SimUser represents a simulated user.
type SimUser struct {
	DeviceID    string
	ProfileID   int64
	Key         int64
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

var (
	activeUsers     = make(map[int64]*SimUser)
	usersMutex      sync.RWMutex
	userGenerators  = make(map[int64]*rand.Rand)
	generatorsMutex sync.RWMutex

	// OSList holds the possible OS values for a user.
	OSList = []string{"android", "ios"}
	// Types holds the possible device type values.
	Types = []string{"smartphone", "tablet", "other"}
	// Models holds the possible device model values.
	Models = []string{"iphone16", "xiaomi12", "pocoM6pro", "samsungS24", "pixel8"}
)

// AddActiveUser adds a user to the active user map.
func AddActiveUser(u *SimUser) {
	usersMutex.Lock()
	defer usersMutex.Unlock()

	if u.ProfileID != 0 {
		u.Key = u.ProfileID
		activeUsers[u.Key] = u
		return
	}
	h := fnv.New64a()
	h.Write([]byte(u.DeviceID))
	tempID := int64(h.Sum64())
	u.Key = tempID
	activeUsers[u.Key] = u
}

// PromoteUserToProfile updates the user's key upon registration.
func PromoteUserToProfile(u *SimUser, profileID int64) {
	usersMutex.Lock()
	defer usersMutex.Unlock()
	if u.Key != 0 {
		delete(activeUsers, u.Key)
	}
	u.ProfileID = profileID
	u.Key = profileID
	activeUsers[u.Key] = u
}

// RemoveActiveUserByKey removes a user from the active map.
func RemoveActiveUserByKey(key int64) {
	usersMutex.Lock()
	delete(activeUsers, key)
	usersMutex.Unlock()
}

// GetActiveUsers returns a slice of all active users.
func GetActiveUsers() []*SimUser {
	usersMutex.RLock()
	defer usersMutex.RUnlock()

	users := make([]*SimUser, 0, len(activeUsers))
	for _, user := range activeUsers {
		users = append(users, user)
	}
	return users
}

// GetUserGenerator returns a random number generator for a user.
func GetUserGenerator(key int64) *rand.Rand {
	generatorsMutex.RLock()
	gen, exists := userGenerators[key]
	generatorsMutex.RUnlock()

	if !exists {
		generatorsMutex.Lock()
		gen = rand.New(rand.NewSource(time.Now().UnixNano() + key))
		userGenerators[key] = gen
		generatorsMutex.Unlock()
	}
	return gen
}

// NewSimUser creates a new simulated user.
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

func randomDeviceID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012xR",
		rand.Uint32(),
		rand.Uint32()&0xffff,
		rand.Uint32()&0xffff,
		rand.Uint32()&0xffff,
		rand.Uint64()&0xffffffffffff,
	)
}
