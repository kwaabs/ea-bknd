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
	feedbackSvc := services.NewFeedbackService(db)
	meterMetricsSvc := services.NewMeterMetricsService(db)
	serviceAreaSvc := services.NewServiceAreaService(db) // ✅ NEW

	// create the auth middleware instance (pass dependencies)
	authMW := mdlwr.NewAuthMiddleware(cfg.JWTPublicKeyPath, authSvc, logr.Logger)

	authHandler := handlers.NewAuthHandler(authSvc, logr, cfg)
	meterHandler := handlers.NewMeterHandler(meterSvc, logr.Logger)
	feedbackHandler := handlers.NewFeedbackHandler(feedbackSvc, logr.Logger)
	meterMetricsHandler := handlers.NewMeterMetricsHandler(meterMetricsSvc, logr.Logger)
	serviceAreaHandler := handlers.NewServiceAreaHandler(serviceAreaSvc, logr.Logger) // ✅ NEW

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

			// In your router setup
			r.Route("/metadata", func(r chi.Router) {
				r.Get("/regions", meterHandler.GetRegions)
				r.Get("/districts", meterHandler.GetDistricts)
				r.Get("/stations", meterHandler.GetStations)
				r.Get("/locations", meterHandler.GetLocations)

				r.Get("/boundary-points", meterHandler.GetBoundaryPoints)
				r.Get("/voltages", meterHandler.GetVoltages)
			})

			// Geometry endpoints
			r.Get("/geometries/districts", meterHandler.GetDistrictGeometries)

			// Timeseries endpoints
			r.Get("/consumption/districts-timeseries", meterHandler.GetDistrictTimeseriesConsumption)

			// Basic meter operations
			r.Get("/", meterHandler.QueryMeters)
			r.Get("/{id}", meterHandler.GetMeterByID)

			// ✅ STATUS ENDPOINTS - NEW OPTIMIZED STRUCTURE
			r.Route("/status", func(r chi.Router) {
				// NEW - Phase 1 (Critical)
				r.Get("/summary", meterHandler.GetMeterStatusSummary)   // < 1 KB
				r.Get("/timeline", meterHandler.GetMeterStatusTimeline) // < 50 KB
				r.Get("/details", meterHandler.GetMeterStatusDetails)   // 25 KB per page

				// Keep existing for backward compatibility
				r.Get("/", meterHandler.GetMeterStatus)             // DEPRECATED
				r.Get("/counts", meterHandler.GetMeterStatusCounts) // DEPRECATED
			})

			// ✅ HEALTH ENDPOINT - NEW (Phase 2, Optional)
			r.Route("/health", func(r chi.Router) {
				r.Get("/metrics", meterHandler.GetMeterHealthMetrics)
				r.Get("/summary", meterHandler.GetMeterHealthSummary)
				r.Get("/summary/details", meterHandler.GetMeterHealthDetails)
			})

			// Keep existing readings routes unchanged
			r.Route("/readings", func(r chi.Router) {
				r.Get("/metrics", meterMetricsHandler.GetMeterMetrics)
				r.Get("/aggregated", meterHandler.GetAggregatedReadings)
				r.Get("/consumption", meterHandler.GetDailyConsumption)
			})

			// ✅ CONSUMPTION ENDPOINTS - ENHANCED
			r.Route("/consumption", func(r chi.Router) {
				// NEW - Phase 2
				r.Get("/by-region", meterHandler.GetConsumptionByRegion)       // Regional supply patterns
				r.Get("/regional-map", meterHandler.GetRegionalMapConsumption) // Regional supply patterns

				// Keep ALL existing routes unchanged
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
				r.Get("/top-bottom-consumers", meterHandler.GetTopBottomConsumers)
			})

			// ✅ NEW: Spatial service area routes
			r.Route("/spatial", func(r chi.Router) {
				r.Get("/", meterHandler.GetMetersWithServiceArea)
				r.Get("/mismatch", meterHandler.GetMeterSpatialMismatch)
				r.Get("/stats", meterHandler.GetMeterSpatialStats)

				// ✅ NEW: Aggregation/count endpoints
				r.Get("/counts", meterHandler.GetMeterSpatialCounts) // Flexible grouping
				r.Get("/counts/by-region", meterHandler.GetMeterSpatialCountsByRegion)
				r.Get("/counts/by-district", meterHandler.GetMeterSpatialCountsByDistrict)
				r.Get("/counts/by-type", meterHandler.GetMeterSpatialCountsByType)
			})
		})

		r.Route("/feedback", func(r chi.Router) {
			r.Post("/", feedbackHandler.CreateFeedback)

			r.Get("/", feedbackHandler.GetAllFeedback)
			r.Get("/user/{email}", feedbackHandler.GetFeedbackByEmail)

			// Get all feedback (paginated)
			r.Get("/feedback/{id}", feedbackHandler.GetFeedbackByID)               // Get single feedback with replies
			r.Patch("/feedback/{id}/status", feedbackHandler.UpdateFeedbackStatus) // Update status
		})

		// ✅ NEW: Service Areas routes
		r.Route("/service-areas", func(r chi.Router) {
			//r.Use(authMW.JWTAuth) // Protect with JWT if needed
			r.Get("/", serviceAreaHandler.GetServiceAreas)            // Get all service areas
			r.Get("/{id}", serviceAreaHandler.GetServiceAreaByID)     // Get single service area
			r.Get("/meta/regions", serviceAreaHandler.GetRegions)     // Get unique regions
			r.Get("/meta/districts", serviceAreaHandler.GetDistricts) // Get unique districts
		})

	})

	return r
}
