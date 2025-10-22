# -----------------------------
# Stage 1: Build the Go binary
# -----------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy module files
COPY go.mod go.sum* ./
RUN go mod download

# Copy source
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o server ./cmd/server

# -----------------------------
# Stage 2: Runtime image
# -----------------------------
FROM alpine:3.20

WORKDIR /app

# Copy the binary
COPY --from=builder /app/server .

# (Optional) Add CA certs for HTTPS/database connections
RUN apk --no-cache add ca-certificates

# Expose your correct port
EXPOSE 8780

# Set environment variable (Coolify respects this)
ENV PORT=8780

# Run the app
CMD ["./server"]
