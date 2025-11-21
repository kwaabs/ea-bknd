package routes

import (
	"bknd-1/internal/auth"
	"bknd-1/internal/config"
	"bknd-1/internal/handlers"
	"bknd-1/internal/logger"
	mdlwr "bknd-1/internal/middleware"
	"bknd-1/internal/services"

	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/uptrace/bun"

	"github.com/go-chi/cors"
)

func NewRouter(db *bun.DB, cfg *config.Config, logr *logger.Logger) http.Handler {
	r := chi.NewRouter()

	// Basic middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	// CORS middleware with config
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// init JWT manager
	jwtMgr, err := auth.NewJWTManager(cfg.JWTPrivateKeyPath, cfg.JWTPublicKeyPath, "yourapp")
	if err != nil {
		logr.Fatal("failed to init jwt manager", zap.Error(err))
	}

	// auth service (service handles DB checks like token_version)
	authSvc := services.NewAuthService(db, jwtMgr, cfg, logr)
	meterSvc := services.NewMeterService(db)
	meterMetricsSvc := services.NewMeterMetricsService(db)

	// create the auth middleware instance (pass dependencies)
	authMW := mdlwr.NewAuthMiddleware(cfg.JWTPublicKeyPath, authSvc, logr.Logger)

	authHandler := handlers.NewAuthHandler(authSvc, logr, cfg)
	meterHandler := handlers.NewMeterHandler(meterSvc, logr.Logger)
	meterMetricsHandler := handlers.NewMeterMetricsHandler(meterMetricsSvc, logr.Logger)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("ok"))
		if err != nil {
			return
		}
	})

	r.Route("/api/v1", func(r chi.Router) {

		r.Route("/auth", func(r chi.Router) {
			// Public routes
			r.Post("/login", authHandler.LoginLocal)
			r.Post("/ldap", authHandler.LoginLDAP)

			// Protected routes
			r.Group(func(r chi.Router) {
				// use the middleware instance's method as middleware
				r.Use(authMW.JWTAuth)
				r.Post("/refresh", authHandler.Refresh)
				r.Post("/logout", authHandler.Logout)
			})
		})

		r.Route("/meters", func(r chi.Router) {
			//r.Use(authMW.JWTAuth) // protect with JWT
			r.Get("/", meterHandler.QueryMeters)
			r.Get("/status", meterHandler.GetMeterStatus)
			r.Get("/status/counts", meterHandler.GetMeterStatusCounts)

			r.Get("/{id}", meterHandler.GetMeterByID)

			r.Route("/readings", func(r chi.Router) {
				//r.Get("/", meterHandler.GetReadings)
				r.Get("/metrics", meterMetricsHandler.GetMeterMetrics)
				r.Get("/aggregated", meterHandler.GetAggregatedReadings)
				r.Get("/consumption", meterHandler.GetDailyConsumption)

			})

			r.Route("/consumption", func(r chi.Router) {
				r.Get("/daily", meterHandler.GetDailyConsumption)
				r.Get("/aggregate", meterHandler.GetAggregatedConsumption)

				r.Get("/daily/regional", meterHandler.GetRegionalBoundaryDailyConsumption)
				r.Get("/aggregate/regional", meterHandler.GetRegionalBoundaryAggregatedConsumption)

				r.Get("/daily/district", meterHandler.GetDistrictBoundaryDailyConsumption)
				r.Get("/aggregate/district", meterHandler.GetDistrictBoundaryAggregatedConsumption)

				r.Get("/daily/bsp", meterHandler.GetBSPDailyConsumption)
				r.Get("/aggregate/bsp", meterHandler.GetBSPAggregatedConsumption)

				r.Get("/daily/pss", meterHandler.GetPSSDailyConsumption)
				r.Get("/aggregate/pss", meterHandler.GetPSSAggregatedConsumption)

				r.Get("/daily/ss", meterHandler.GetSSDailyConsumption)
				r.Get("/aggregate/ss", meterHandler.GetSSAggregatedConsumption)

				r.Get("/daily/feeder-trafo", meterHandler.GetFeederDailyConsumption)
				r.Get("/aggregate/feeder-trafo", meterHandler.GetFeederAggregatedConsumption)

				r.Get("/daily/dtx", meterHandler.GetDTXDailyConsumption)
				r.Get("/aggregate/dtx", meterHandler.GetDTXAggregatedConsumption)

			})
		})

	})

	return r
}
