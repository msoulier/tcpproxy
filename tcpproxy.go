package main
// vim: ft=go ts=4 sw=4 et ai:

import (
    "fmt"
    "net"
    "os"
    "github.com/op/go-logging"
    "github.com/jessevdk/go-flags"
    "strings"
    "time"
    "errors"
)

var (
    log *logging.Logger
    debug bool
    opts struct {
        Listenif string `short:"l" long:"listenif" description:"Listen interface" default:"0.0.0.0"`
        Debug bool `short:"d" long:"debug" description:"Enable debug logging"`
        Proxies []string `short:"p" long:"proxy" description:"Define proxy handler <localport:remotehost:remoteport>" required:"true"`
    }
)

func parse_options() {
    _, err := flags.Parse(&opts)
    if err != nil {
        fmt.Fprintf(os.Stderr, fmt.Sprintf("See -h|--help for help\n"))
        os.Exit(1)
    }
    format := logging.MustStringFormatter(
        `%{time:2006-01-02 15:04:05.000-0700} %{level} [%{shortfile}] %{message}`,
    )
    stderrBackend := logging.NewLogBackend(os.Stderr, "", 0)
    stderrFormatter := logging.NewBackendFormatter(stderrBackend, format)
    stderrBackendLevelled := logging.AddModuleLevel(stderrFormatter)
    logging.SetBackend(stderrBackendLevelled)
    if opts.Debug {
        stderrBackendLevelled.SetLevel(logging.DEBUG, "tcpserver")
    } else {
        stderrBackendLevelled.SetLevel(logging.INFO, "tcpserver")
    }
    log = logging.MustGetLogger("tcpserver")
    log.Debug("debug logging enabled")

    // Log the defined proxies
    for _, p := range opts.Proxies {
        log.Debugf("proxy: %s", p)
    }
    if len(opts.Proxies) > 1 {
        log.Error("Multiple proxies are not yet supported")
        os.Exit(1)
    }
}

// Handles incoming requests.
func echoServerHandler(conn net.Conn) {
    // Make a buffer to hold incoming data.
    buf := make([]byte, 1024)
    // Read the incoming connection into the buffer.
    reqLen, err := conn.Read(buf)
    log.Debugf("received %d bytes", reqLen)
    if err != nil {
        fmt.Println("Error reading:", err.Error())
    }
    // Send a response back to person contacting us.
    //conn.Write([]byte("Message received."))
    conn.Write(buf)
    // Close the connection when you're done with it.
    conn.Close()
}

func copyToSocket(read_conn, write_conn net.Conn, joinchan chan error) {
    // Read from read_conn. If anything is read, write it to write_conn.
    for {
        // Make a buffer to hold incoming data.
        buf := make([]byte, 1024)
        // Read the incoming connection into the buffer.
        req_len, err := read_conn.Read(buf)
        log.Debugf("received %d bytes", req_len)
        if err != nil {
            log.Errorf("Error reading: %s", err)
            joinchan <- err
            return
        }
        write_conn.Write(buf)
    }
}

func proxyHandler(listen_conn net.Conn,
                  remotehost, remoteport string,
                  joinchan chan error) {
    log.Infof("setting up proxy to %s:%s", remotehost, remoteport)
    defer listen_conn.Close()
    tcpAddr, err := net.ResolveTCPAddr("tcp",
                                       fmt.Sprintf("%s:%d",
                                                   remotehost,
                                                   remoteport))
    if err != nil {
        log.Errorf("resolution error: %s", err)
        joinchan <- err
        return
    }
    if remote_conn, err := net.DialTCP("tcp", nil, tcpAddr); err != nil {
        log.Errorf("connect error: %s", err)
        joinchan <- err
        return
    } else {
        log.Infof("connected")
        defer remote_conn.Close()
        copy_joinchan := make(chan error)
        // Start two goroutines for non-blocking I/O in both directions
        go copyToSocket(listen_conn, remote_conn, copy_joinchan)
        go copyToSocket(remote_conn, listen_conn, copy_joinchan)
        // And block on the join channel. If one end quits, then we're
        // done.
        err := <- copy_joinchan
        // FIXME: shut down the other goroutine
        log.Errorf("one end of the connection dropped: %s", err)
        // FIXME: identify which end dropped - separate channels?
        joinchan <- err
        return
    }
}

// Proxy handler
func initialProxyHandler(listenport, remotehost, remoteport string,
                         joinchan chan error) {
    log.Infof("initialProxyHandler: %s:%s:%s",
        listenport, remotehost, remoteport)
    sub_handler_joinchan := make(chan error)
    gr_count := 0
    // Listen for incoming connections.
    listener, err := net.Listen("tcp", opts.Listenif + ":" + listenport)
    if err != nil {
        fmt.Println("Error listening:", err.Error())
        joinchan <- err
        return
    }
    // Close the listener when the application closes.
    tcplistener := listener.(*net.TCPListener)
    defer tcplistener.Close()
    log.Infof("Listening on %s:%s", opts.Listenif, listenport)
    for {
        // Let the tcplistener come out of Accept so we can check for
        // sub-goroutines exiting.
        tcplistener.SetDeadline(time.Now().Add(time.Second * 5))
        // Listen for an incoming connection.
        new_listenconn, err := tcplistener.Accept()
        if err != nil {
            if errors.Is(err, os.ErrDeadlineExceeded) {
                log.Debug("timeout on Accept")
            } else {
                log.Errorf("Error accepting: %s", err.Error())
                joinchan <- err
                return
            }
        } else {
            log.Infof("new connection: %v", new_listenconn)
            // Handle connections in a new goroutine.
            go proxyHandler(new_listenconn, remotehost, remoteport, sub_handler_joinchan)
            gr_count++
            log.Debugf("connection handler goroutine count: %d", gr_count)
        }
        // Check the join channel
        select {
            case err := <- sub_handler_joinchan:
                log.Infof("connection handler returned: %s", err)
                gr_count--
                log.Infof("connection goroutines count: %d", gr_count)
            default:
                log.Info("nothing waiting on join channel")
        }
    }
}

func main() {
    parse_options()

    joinchan := make(chan error)
    gr_count := 0
    // Set up proxies - only one for now but plan for the future
    for _, proxy := range opts.Proxies {
        log.Debugf("setting up proxy %s", proxy)
        pieces := strings.Split(proxy, ":")
        log.Debugf("pieces: %v", pieces)
        if len(pieces) != 3 {
            log.Errorf("invalid proxy syntax: %s", proxy)
            continue
        }
        listenport := pieces[0]
        remotehost := pieces[1]
        remoteport := pieces[2]
        // Each proxy has its own parent goroutine
        go initialProxyHandler(listenport, remotehost, remoteport, joinchan)
        gr_count++
        log.Infof("proxy handler goroutine count: %d", gr_count)
    }
    // Block until all goroutines have returned.
    for {
        err := <- joinchan
        gr_count--
        log.Infof("handler goroutine exited, count is now %d", gr_count)
        if err != nil {
            log.Errorf("handler reported error: %s", err)
        } else {
            log.Info("handler returned without error")
        }
        if gr_count == 0 {
            log.Info("all handlers returned - shutting down")
            break
        }
    }
    os.Exit(0)
}
