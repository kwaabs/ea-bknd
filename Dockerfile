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

# Create a non-root user and switch to it for security
RUN adduser -D -g '' appuser
USER appuser

# Add CA certs
RUN apk --no-cache add ca-certificates

# Set a non-root working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/server .

# Copy the keys (if not sensitive)
COPY --from=builder --chown=appuser:appuser /app/keys ./keys

# Expose your correct port
EXPOSE 8780

# Set environment variable
ENV PORT=8780

# Use ENTRYPOINT to make the container behave as the executable
ENTRYPOINT ["./server"]