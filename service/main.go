package main

import (
    "bytes"
    "context"
    "encoding/json"
    "io"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
    "github.com/gin-gonic/gin"
)

const SEGMENT_BYTES_CNT = 100

type Sgt struct {
    Payload         string `json:"payload"`
    Time            string `json:"time"`
    SegmentsCount   int    `json:"segments_count"`
    SegmentNum      int    `json:"segment_num"`
}

type Service struct {
    channelURL string
}

func New(channelURL string) (*Service, error) {
    s := Service{}
    s.channelURL = channelURL
    return &s, nil
}

func (s *Service) Send(c *gin.Context) {
    data, err := io.ReadAll(c.Request.Body)
    if err != nil {
        log.Fatal("failed to read message:", err)
        c.AbortWithStatus(http.StatusInternalServerError)
        return
    }
    log.Println("incoming message: ", string(data))
    start := 0
    counter := 0
    size := len(data)
    segmentsCount := 1 + (size - 1) / SEGMENT_BYTES_CNT
    now := time.Now().Format(time.RFC3339Nano)
    for ; start < size; start += SEGMENT_BYTES_CNT {
        var sgt Sgt
        sgt.Payload = string(data[start:min(start + SEGMENT_BYTES_CNT, size)])
        sgt.Time = now
        sgt.SegmentsCount = segmentsCount
        sgt.SegmentNum = counter
        payload, err := json.Marshal(sgt)
        if err != nil {
            log.Fatal("failed to marshal segment: ", err)
            c.AbortWithStatus(http.StatusInternalServerError)
            return
        }
        log.Print("sending json: ", string(payload))
        resp, err := http.Post(s.channelURL + "/transfer", "application/json", bytes.NewReader(payload))
        if err != nil {
            log.Fatal("channel service unavailable: ", err)
            c.AbortWithStatus(http.StatusInternalServerError)
            return
        }
        if resp.StatusCode != 200 {
            log.Fatal("channel service failed: ", resp.Status)
            c.AbortWithStatus(http.StatusInternalServerError)
            return
        }
        counter++
    }
    c.Status(http.StatusOK)
}

func main() {
    port := os.Getenv("port")
    channelURL := "http://" + os.Getenv("channelURL")
    for i := 5; i > 0; i-- {
        log.Print("service sleeping for ", i, " seconds")
        time.Sleep(1 * time.Second)
    }
    log.Print("serice starting at :", port, " with channelURL=", channelURL)
    s, err := New(channelURL)
    if err != nil {
        log.Fatal("failed: ", err)
        return
    }
    r := gin.New()
    r.POST("/send", s.Send)
    srv := &http.Server{
        Addr: ":" + port,
        Handler: r.Handler(),
    }
    go func() {
        log.Println("service started")
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("listen: %s\n", err)
        }
    }()
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit
    log.Println("shutdown service ...")
    ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        log.Fatal("service shutdown:", err)
    }
    select {
    case <-ctx.Done():
        log.Println("shutdown timeout has expired")
    }
    log.Println("service exiting")
}
