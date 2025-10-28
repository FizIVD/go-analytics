package main

import (
	"context"
	"flag"
	"go-event-api/internal/generator"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	var usersPerDay, days, shards int
	flag.IntVar(&usersPerDay, "u", 1000, "Новых пользователей в день")
	flag.IntVar(&days, "d", 1, "Количество дней")
	flag.IntVar(&shards, "s", 8, "Количество шардов")
	flag.Parse()

	endpoint := getenv("API_URL", "http://localhost:8080/event")
	timeScale := time.Minute

	var totalEvents, totalInstalls, totalRegistrations atomic.Int64

	scheduler := generator.NewShardedEventGenerator(shards, endpoint, &totalEvents)
	sim := generator.NewSimulation(scheduler, timeScale, &totalInstalls, &totalRegistrations)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	log.Printf("Starting simulation: %d users per day for %d days", usersPerDay, days)
	log.Printf("Time scale: 1 minute = 1 day")
	log.Printf("Target endpoint: %s", endpoint)

	baseTime := time.Now()
	scheduler.Start(ctx)

	for day := 0; day < days; day++ {
		dayStart := baseTime.Add(time.Duration(day) * timeScale)
		log.Printf("Processing day %d...", day+1)

		// Create new users for the day
		for i := 0; i < usersPerDay; i++ {
			u := generator.NewSimUser(r, day)
			generator.AddActiveUser(u)
			sim.ScheduleUserLifecycle(u, day, dayStart, r)
		}

		// Schedule activity for existing users
		sim.ScheduleDailyActivity(day, dayStart)

		activeUserList := generator.GetActiveUsers()
		regCount := 0
		for _, u := range activeUserList {
			if u.ProfileID != 0 && !u.Churned {
				regCount++
			}
		}
		log.Printf("Day %d scheduled. Active users: %d (%d registered)", day+1, len(activeUserList), regCount)


		// Wait until the next simulated day begins
		nextDayStart := baseTime.Add(time.Duration(day+1) * timeScale)
		now := time.Now()
		if now.Before(nextDayStart) {
			time.Sleep(nextDayStart.Sub(now))
		}

		// Clear any remaining events from the queue at the end of the day
		scheduler.ClearShards()
	}

	cancel()
	scheduler.Wait()

	log.Printf("=== SIMULATION FINISHED ===")
	log.Printf("Total events sent: %d", totalEvents.Load())
	log.Printf("Total installs: %d", totalInstalls.Load())
	log.Printf("Total registrations: %d", totalRegistrations.Load())
	log.Printf("Active users remaining: %d", len(generator.GetActiveUsers()))
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
