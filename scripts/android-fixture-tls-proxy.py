#!/usr/bin/env python3
"""Loopback-only TLS proxy for Android integration tests, never a production server."""
import argparse, http.client, http.server, json, ssl

LIMIT = 65 * 1024 * 1024
HOP = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
       'te', 'trailer', 'transfer-encoding', 'upgrade', 'content-length'}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--listen-port', type=int, required=True)
    parser.add_argument('--upstream-port', type=int, required=True)
    parser.add_argument('--cert', required=True)
    parser.add_argument('--key', required=True)
    args = parser.parse_args()
    if not (1024 <= args.listen_port <= 65535 and 1024 <= args.upstream_port <= 65535):
        raise SystemExit('nonprivileged loopback ports required')

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *unused):
            pass  # Never retain URL capabilities, Authorization, or cookies.
        def do_GET(self):
            self.forward()
        def do_POST(self):
            self.forward()
        def forward(self):
            upstream = None
            self.connection.settimeout(60)
            try:
                if not self.path.startswith('/') or self.path.startswith('//'):
                    self.send_error(400)
                    return
                if self.headers.get('Transfer-Encoding'):
                    self.send_error(411)
                    return
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 <= length <= LIMIT:
                    self.send_error(413)
                    return
                body = self.rfile.read(length)
                if len(body) != length:
                    self.close_connection = True
                    return
                headers = {key: val for key, val in self.headers.items() if key.lower() not in HOP}
                headers['Content-Length'] = str(length)
                upstream = http.client.HTTPConnection('127.0.0.1', args.upstream_port, timeout=45)
                upstream.request(self.command, self.path, body=body, headers=headers)
                response = upstream.getresponse()
                data = response.read(LIMIT + 1)
                if len(data) > LIMIT:
                    raise ValueError('fixture response exceeds budget')
                self.send_response(response.status, response.reason)
                for key, val in response.getheaders():
                    if key.lower() not in HOP:
                        self.send_header(key, val)
                self.send_header('Content-Length', str(len(data)))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.write(data)
            except (OSError, ValueError, http.client.HTTPException):
                self.close_connection = True
            finally:
                if upstream:
                    upstream.close()
                self.close_connection = True

    server = http.server.ThreadingHTTPServer(('127.0.0.1', args.listen_port), Handler)
    server.daemon_threads = True
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(args.cert, args.key)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    server.serve_forever(poll_interval=0.2)

if __name__ == '__main__':
    main()
