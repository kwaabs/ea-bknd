package main

import (
	"bknd-1/internal/config"
	"bknd-1/internal/database"
	"bknd-1/internal/logger"
	"bknd-1/internal/routes"
	"context"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.Load()
	logr := logger.New(cfg)
	db, err := database.New(cfg.DatabaseURL, cfg)
	if err != nil {
		logr.Fatal("failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	r := routes.NewRouter(db, cfg, logr)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logr.Info("server started", zap.String("port", cfg.Port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logr.Fatal("server failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logr.Info("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logr.Fatal("server forced to shutdown", zap.Error(err))
	}

	_ = db.Close()
	logr.Info("server exited gracefully")
}
