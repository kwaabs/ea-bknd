# -----------------------------
# Stage 1: Build the Go binary
# -----------------------------
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod (required)
COPY go.mod ./

# Copy go.sum only if it exists (avoids build failure)
# The wildcard * handles missing files gracefully
COPY go.sum* ./

# Download dependencies
RUN go mod download

# Copy the rest of your source code
COPY . .

# Build the Go binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o server ./cmd/server

# -----------------------------
# Stage 2: Minimal runtime image
# -----------------------------
FROM scratch

# Copy compiled binary
COPY --from=builder /app/server /server

# Expose port
EXPOSE 8780

# Run binary
ENTRYPOINT ["/server"]