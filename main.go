package main

import (
    "bufio"
    "bytes"
    "crypto/rand"
    "encoding/base64"
    "encoding/binary"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "time"
)

const (
    actionUUID   = "com.lathanor.recklessrp.status.action"
    joinID       = "3yg6jzb"
    maxDefault   = 400
    refreshEvery = 3 * time.Second
    httpTimeout  = 7 * time.Second
)

var statusURLs = []string{
    "https://frontend.cfx-services.net/api/servers/single/" + joinID,
    "https://servers.redm.net/servers/detail/" + joinID + "?_data=routes%2Fservers.detail",
    "https://servers.redm.net/servers/detail/" + joinID + "?_data=routes%2Fservers.detail.%24id",
    "https://servers.redm.net/servers/detail/" + joinID + "?_data=routes%2Fservers.detail.%24serverId",
    "https://servers.redm.net/api/servers/single/" + joinID,
    "http://178.208.177.44:30120/dynamic.json",
    "http://178.208.177.44:30120/players.json",
}

type Args struct{ Port, PluginUUID, RegisterEvent, Info string }
type SDMessage struct {
    Event   string          `json:"event"`
    Context string          `json:"context,omitempty"`
    Action  string          `json:"action,omitempty"`
    Payload json.RawMessage `json:"payload,omitempty"`
}
type WSConn struct{ c net.Conn; mu sync.Mutex }
type Status struct{ Online bool; Clients, Max int; Source, Err string }

type apiResp struct {
    EndPoint string `json:"EndPoint"`
    Data struct {
        Hostname string `json:"hostname"`
        Clients int `json:"clients"`
        SvMaxClients int `json:"sv_maxclients"`
        SvMaxclients int `json:"svMaxclients"`
        SelfReportedClients int `json:"selfReportedClients"`
        Vars map[string]string `json:"vars"`
        Players []any `json:"players"`
    } `json:"Data"`
}

type dynamicResp struct {
    Clients int `json:"clients"`
    SvMaxClients int `json:"sv_maxclients"`
    MaxClients int `json:"maxclients"`
    Hostname string `json:"hostname"`
}

var logger *log.Logger
var lastGood Status
var failures int

func initLog() {
    exe, _ := os.Executable()
    dir := filepath.Dir(exe)
    _ = os.MkdirAll(filepath.Join(dir, "logs"), 0755)
    f, err := os.OpenFile(filepath.Join(dir, "logs", "recklessrp-status.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil { logger = log.New(io.Discard, "", 0); return }
    logger = log.New(f, "", log.LstdFlags)
}

func parseArgs() Args {
    a := Args{}
    for i := 1; i < len(os.Args)-1; i++ {
        switch os.Args[i] {
        case "-port": a.Port = os.Args[i+1]
        case "-pluginUUID": a.PluginUUID = os.Args[i+1]
        case "-registerEvent": a.RegisterEvent = os.Args[i+1]
        case "-info": a.Info = os.Args[i+1]
        }
    }
    return a
}

func wsDial(address string) (*WSConn, error) {
    u, err := url.Parse(address); if err != nil { return nil, err }
    c, err := net.DialTimeout("tcp", u.Host, 5*time.Second); if err != nil { return nil, err }
    path := u.RequestURI(); if path == "" { path = "/" }
    keyBytes := make([]byte, 16); _, _ = rand.Read(keyBytes)
    key := base64.StdEncoding.EncodeToString(keyBytes)
    req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, u.Host, key)
    if _, err := c.Write([]byte(req)); err != nil { c.Close(); return nil, err }
    br := bufio.NewReader(c)
    status, err := br.ReadString('\n'); if err != nil { c.Close(); return nil, err }
    if !strings.Contains(status, "101") { c.Close(); return nil, fmt.Errorf("websocket upgrade failed: %s", strings.TrimSpace(status)) }
    for { line, err := br.ReadString('\n'); if err != nil { c.Close(); return nil, err }; if line == "\r\n" { break } }
    return &WSConn{c: c}, nil
}

func (w *WSConn) SendText(s string) error {
    w.mu.Lock(); defer w.mu.Unlock()
    payload := []byte(s); l := len(payload)
    header := []byte{0x81}
    if l < 126 { header = append(header, 0x80|byte(l))
    } else if l <= 65535 { header = append(header, 0x80|126, byte(l>>8), byte(l))
    } else { header = append(header, 0x80|127); var b [8]byte; binary.BigEndian.PutUint64(b[:], uint64(l)); header = append(header, b[:]...) }
    mask := make([]byte, 4); _, _ = rand.Read(mask)
    masked := make([]byte, l)
    for i := range payload { masked[i] = payload[i] ^ mask[i%4] }
    _, err := w.c.Write(append(append(header, mask...), masked...))
    return err
}

func (w *WSConn) ReadText() (string, error) {
    var h [2]byte
    if _, err := io.ReadFull(w.c, h[:]); err != nil { return "", err }
    opcode := h[0] & 0x0f
    masked := (h[1] & 0x80) != 0
    l := uint64(h[1] & 0x7f)
    if l == 126 { var b [2]byte; if _, err := io.ReadFull(w.c, b[:]); err != nil { return "", err }; l = uint64(binary.BigEndian.Uint16(b[:]))
    } else if l == 127 { var b [8]byte; if _, err := io.ReadFull(w.c, b[:]); err != nil { return "", err }; l = binary.BigEndian.Uint64(b[:]) }
    var mask [4]byte
    if masked { if _, err := io.ReadFull(w.c, mask[:]); err != nil { return "", err } }
    payload := make([]byte, l)
    if _, err := io.ReadFull(w.c, payload); err != nil { return "", err }
    if masked { for i := range payload { payload[i] ^= mask[i%4] } }
    if opcode == 0x8 { return "", io.EOF }
    if opcode == 0x9 { _ = w.sendPong(payload); return w.ReadText() }
    if opcode != 0x1 { return "", nil }
    return string(payload), nil
}
func (w *WSConn) sendPong(payload []byte) error { w.mu.Lock(); defer w.mu.Unlock(); header:=[]byte{0x8A,0x80|byte(len(payload))}; mask:=make([]byte,4); _,_=rand.Read(mask); masked:=make([]byte,len(payload)); for i:=range payload { masked[i]=payload[i]^mask[i%4] }; _,err:=w.c.Write(append(append(header,mask...),masked...)); return err }

func httpGetBytes(u string) ([]byte, int, error) {
    req, err := http.NewRequest("GET", u, nil); if err != nil { return nil, 0, err }
    req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")
    req.Header.Set("Accept", "application/json,text/plain,*/*")
    req.Header.Set("Referer", "https://servers.redm.net/servers/detail/"+joinID)
    req.Header.Set("Origin", "https://servers.redm.net")
    req.Header.Set("X-Requested-With", "XMLHttpRequest")
    client := &http.Client{Timeout: httpTimeout}
    resp, err := client.Do(req); if err != nil { return nil, 0, err }
    defer resp.Body.Close()
    b, err := io.ReadAll(io.LimitReader(resp.Body, 25*1024*1024)); if err != nil { return nil, resp.StatusCode, err }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 { return b, resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode) }
    return b, resp.StatusCode, nil
}

func parseStatusFromBody(b []byte, source string) Status {
    trimmed := bytes.TrimSpace(b)
    if len(trimmed) == 0 { return Status{Err: source+": empty body"} }

    // players.json direct endpoint
    if trimmed[0] == '[' {
        var arr []any
        if json.Unmarshal(trimmed, &arr) == nil { return Status{Online:true, Clients:len(arr), Max:maxDefault, Source:source} }
    }

    var ar apiResp
    if json.Unmarshal(trimmed, &ar) == nil {
        c := ar.Data.Clients
        if c == 0 && ar.Data.SelfReportedClients > 0 { c = ar.Data.SelfReportedClients }
        if c == 0 && len(ar.Data.Players) > 0 { c = len(ar.Data.Players) }
        m := ar.Data.SvMaxClients
        if m <= 0 { m = ar.Data.SvMaxclients }
        if m <= 0 && ar.Data.Vars != nil {
            for _, k := range []string{"sv_maxClients","sv_maxclients","svMaxclients","maxclients"} {
                if v := ar.Data.Vars[k]; v != "" { if n, err := strconv.Atoi(v); err == nil { m = n; break } }
            }
        }
        if m <= 0 { m = maxDefault }
        if ar.EndPoint != "" || ar.Data.Hostname != "" || ar.Data.Vars != nil || len(ar.Data.Players) > 0 || ar.Data.SvMaxClients > 0 || ar.Data.SvMaxclients > 0 {
            return Status{Online:true, Clients:c, Max:m, Source:source}
        }
    }

    var dr dynamicResp
    if json.Unmarshal(trimmed, &dr) == nil && (dr.Clients >= 0) && (dr.SvMaxClients > 0 || dr.MaxClients > 0 || dr.Hostname != "") {
        m := dr.SvMaxClients; if m <= 0 { m = dr.MaxClients }; if m <= 0 { m = maxDefault }
        return Status{Online:true, Clients:dr.Clients, Max:m, Source:source}
    }

    // last-resort regex over JSON or embedded text
    s := string(trimmed)
    c := -1; m := -1
    for _, re := range []*regexp.Regexp{
        regexp.MustCompile(`"clients"\s*:\s*([0-9]+)`),
        regexp.MustCompile(`"selfReportedClients"\s*:\s*([0-9]+)`),
    } { if mm := re.FindStringSubmatch(s); len(mm) == 2 { c, _ = strconv.Atoi(mm[1]); break } }
    for _, re := range []*regexp.Regexp{
        regexp.MustCompile(`"sv_maxclients"\s*:\s*([0-9]+)`),
        regexp.MustCompile(`"svMaxclients"\s*:\s*([0-9]+)`),
        regexp.MustCompile(`"sv_maxClients"\s*:\s*"?([0-9]+)"?`),
    } { if mm := re.FindStringSubmatch(s); len(mm) == 2 { m, _ = strconv.Atoi(mm[1]); break } }
    if c >= 0 { if m <= 0 { m = maxDefault }; return Status{Online:true, Clients:c, Max:m, Source:source} }

    snip := s
    if len(snip) > 140 { snip = snip[:140] }
    return Status{Err: source+": parse failed; snippet="+strings.ReplaceAll(snip, "\n", " ")}
}

func fetchStatus() Status {
    errs := []string{}
    for _, u := range statusURLs {
        b, _, err := httpGetBytes(u)
        if err != nil { errs = append(errs, u+": "+err.Error()); continue }
        st := parseStatusFromBody(b, u)
        if st.Online { failures = 0; lastGood = st; return st }
        if st.Err != "" { errs = append(errs, st.Err) }
    }
    failures++
    if lastGood.Online && failures <= 2 {
        st := lastGood
        st.Source = "last-known"
        st.Err = strings.Join(errs, " | ")
        return st
    }
    return Status{Online:false, Clients:0, Max:maxDefault, Err:strings.Join(errs, " | ")}
}

func sendJSON(w *WSConn, event, context string, payload any) {
    b, _ := json.Marshal(map[string]any{"event":event, "context":context, "payload":payload})
    if err := w.SendText(string(b)); err != nil && logger != nil { logger.Println("send error:", err) }
}

func applyStatus(w *WSConn, context string, s Status) {
    state := 0
    title := "OFF"
    if s.Online { state = 1; title = fmt.Sprintf("%d/%d", s.Clients, s.Max) }
    sendJSON(w, "setState", context, map[string]any{"state":state})
    sendJSON(w, "setTitle", context, map[string]any{"title":title, "target":0})
    if logger != nil { logger.Printf("online=%v clients=%d max=%d state=%d source=%s err=%s", s.Online, s.Clients, s.Max, state, s.Source, s.Err) }
}

func monitor(w *WSConn, context string, stop <-chan struct{}) {
    applyStatus(w, context, fetchStatus())
    ticker := time.NewTicker(refreshEvery); defer ticker.Stop()
    for { select { case <-ticker.C: applyStatus(w, context, fetchStatus()); case <-stop: return } }
}

func main() {
    initLog()
    a := parseArgs()
    if logger != nil { logger.Println("START", strings.Join(os.Args, " ")) }
    if a.Port == "" || a.PluginUUID == "" || a.RegisterEvent == "" { if logger != nil { logger.Println("missing Stream Deck args") }; return }
    ws, err := wsDial("ws://127.0.0.1:"+a.Port)
    if err != nil { if logger != nil { logger.Println("ws dial:", err) }; return }
    rb, _ := json.Marshal(map[string]string{"event":a.RegisterEvent, "uuid":a.PluginUUID})
    _ = ws.SendText(string(rb))
    stops := map[string]chan struct{}{}
    var mu sync.Mutex
    for {
        txt, err := ws.ReadText(); if err != nil { if logger != nil { logger.Println("read:", err) }; return }
        if txt == "" { continue }
        var m SDMessage; if err := json.Unmarshal([]byte(txt), &m); err != nil { continue }
        if m.Action != "" && m.Action != actionUUID { continue }
        switch m.Event {
        case "willAppear":
            mu.Lock(); if _, ok := stops[m.Context]; !ok { ch := make(chan struct{}); stops[m.Context] = ch; go monitor(ws, m.Context, ch) }; mu.Unlock()
        case "willDisappear":
            mu.Lock(); if ch, ok := stops[m.Context]; ok { close(ch); delete(stops, m.Context) }; mu.Unlock()
        case "keyDown":
            applyStatus(ws, m.Context, fetchStatus())
        }
    }
}
