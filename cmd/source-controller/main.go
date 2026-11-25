// Command source-controller runs the FlowAI Source service: the Source
// catalog and settings, webhook intake, polling and per-Source counts.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandops/flowai-source-controller/internal/app"
	"github.com/pandops/flowai-source-controller/internal/config"
)

func main() {
	path := flag.String("config", "/etc/flowai/config.yaml", "configuration file")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "source-controller")
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "source-controller: invalid configuration:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg, log); err != nil {
		log.Error("source-controller stopped", "error", err.Error())
		os.Exit(1)
	}
}
