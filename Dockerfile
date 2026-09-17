FROM golang:1.26.4 AS builder
WORKDIR /base
COPY ./main.go ./index.html ./style.css ./script.js ./go.mod ./go.sum /base/
COPY ./migrations /base/migrations
RUN go build -o telega
#EXPOSE 8082

FROM alpine
WORKDIR /app
COPY --from=builder ./base/telega ./base/index.html ./base/style.css ./base/script.js /app/
COPY --from=builder /base/migrations /app/migrations
CMD ["/app/telega"]
#EXPOSE 8082