FROM golang:1.26.4 AS builder
WORKDIR /base
COPY ./go.mod ./go.sum /base/
RUN go mod download
COPY ./cmd /base/cmd
COPY ./internal /base/internal
COPY ./index.html ./style.css ./script.js /base/
COPY ./migrations /base/migrations
RUN CGO_ENABLED=0 go build -o telega ./cmd/telega
#EXPOSE 8082

FROM alpine
WORKDIR /app
COPY --from=builder ./base/telega ./base/index.html ./base/style.css ./base/script.js /app/
COPY --from=builder /base/migrations /app/migrations
CMD ["/app/telega"]
#EXPOSE 8082