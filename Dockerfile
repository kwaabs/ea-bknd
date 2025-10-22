# -----------------------------
# Stage 1: Build the Go binary
# -----------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go.mod (required)
COPY go.mod ./

# Copy go.sum if it exists
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

# Copy binary from builder
COPY --from=builder /app/server /server

# Expose port
EXPOSE 8780

# Run the app
ENTRYPOINT ["/server"]
