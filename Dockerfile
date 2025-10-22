# -----------------------------
# Stage 1: Build the Go binary
# -----------------------------
FROM golang:1.23-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first for dependency caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of your source code
COPY . .

# Build the Go binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o server ./cmd/server

# -----------------------------
# Stage 2: Minimal runtime image
# -----------------------------
FROM scratch

# Copy binary from builder
COPY --from=builder /app/server /server

# Expose application port (update if your app uses a different port)
EXPOSE 8080

# Run the app
ENTRYPOINT ["/server"]