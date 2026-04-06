#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cmocka.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_engine_api.h"

typedef struct test_value_doc {
  lonejson_int64 value;
} test_value_doc;

static const lonejson_field test_value_fields[] = {
    LONEJSON_FIELD_I64(test_value_doc, value, "value")};

LONEJSON_MAP_DEFINE(test_value_map, test_value_doc, test_value_fields);

typedef struct test_request_capture {
  char *data;
  size_t length;
  size_t capacity;
} test_request_capture;

typedef struct https_expectation {
  const char *method;
  const char *path;
  const char *const *required_headers;
  size_t required_header_count;
  const char *const *required_body_substrings;
  size_t required_body_substring_count;
  int expect_empty_body;
  int response_status;
  const char *const *response_headers;
  size_t response_header_count;
  const char *response_body;
  const char *expected_client_cn;
} https_expectation;

typedef struct https_tls_material {
  char temp_dir[PATH_MAX];
  char client_bundle_path[PATH_MAX];
  X509 *ca_cert;
  EVP_PKEY *ca_key;
  X509 *server_cert;
  EVP_PKEY *server_key;
} https_tls_material;

typedef struct https_testserver {
  int listener_fd;
  unsigned short port;
  SSL_CTX *ssl_ctx;
  pthread_t thread;
  const https_expectation *expectations;
  size_t expectation_count;
  size_t handled_count;
  char failure_message[1024];
} https_testserver;

typedef struct subscribe_capture {
  int begin_calls;
  int end_calls;
  char message_id[64];
  char payload[256];
  size_t payload_length;
} subscribe_capture;

static int watch_event_sink(void *context, const lc_engine_queue_watch_event *event,
                            lc_engine_error *error) {
  (void)context;
  (void)event;
  (void)error;
  return 1;
}

static long next_test_serial(void) {
  static long serial = 1L;
  return serial++;
}

static void set_failure(https_testserver *server, const char *format, ...) {
  va_list args;

  if (server->failure_message[0] != '\0') {
    return;
  }
  va_start(args, format);
  vsnprintf(server->failure_message, sizeof(server->failure_message), format,
            args);
  va_end(args);
}

static int buffer_append(test_request_capture *capture, const void *bytes,
                         size_t count) {
  char *next;
  size_t required;
  size_t capacity;

  required = capture->length + count + 1U;
  capacity = capture->capacity == 0U ? 1024U : capture->capacity;
  while (capacity < required) {
    capacity *= 2U;
  }
  if (capacity != capture->capacity) {
    next = (char *)realloc(capture->data, capacity);
    if (next == NULL) {
      return 0;
    }
    capture->data = next;
    capture->capacity = capacity;
  }
  memcpy(capture->data + capture->length, bytes, count);
  capture->length += count;
  capture->data[capture->length] = '\0';
  return 1;
}

static void buffer_cleanup(test_request_capture *capture) {
  free(capture->data);
  memset(capture, 0, sizeof(*capture));
}

static char *make_repeat_json_body(const char *prefix, const char *suffix,
                                   size_t payload_len, char fill) {
  size_t prefix_len;
  size_t suffix_len;
  char *body;

  prefix_len = strlen(prefix);
  suffix_len = strlen(suffix);
  body = (char *)malloc(prefix_len + payload_len + suffix_len + 1U);
  if (body == NULL) {
    return NULL;
  }
  memcpy(body, prefix, prefix_len);
  memset(body + prefix_len, fill, payload_len);
  memcpy(body + prefix_len + payload_len, suffix, suffix_len);
  body[prefix_len + payload_len + suffix_len] = '\0';
  return body;
}

static const char *find_header_end(const char *data, size_t length) {
  size_t index;

  if (length < 4U) {
    return NULL;
  }
  for (index = 0U; index + 4U <= length; ++index) {
    if (memcmp(data + index, "\r\n\r\n", 4U) == 0) {
      return data + index;
    }
  }
  return NULL;
}

static long parse_content_length(const char *headers) {
  const char *cursor;

  cursor = headers;
  while (cursor != NULL && *cursor != '\0') {
    const char *line_end;

    if (strncasecmp(cursor, "Content-Length:", 15U) == 0) {
      cursor += 15U;
      while (*cursor == ' ' || *cursor == '\t') {
        ++cursor;
      }
      return strtol(cursor, NULL, 10);
    }
    line_end = strstr(cursor, "\r\n");
    if (line_end == NULL) {
      break;
    }
    cursor = line_end + 2;
  }
  return 0L;
}

static int request_uses_chunked_transfer_encoding(const char *headers) {
  return strstr(headers, "Transfer-Encoding: chunked") != NULL;
}

static int chunked_request_complete(const char *body, size_t length) {
  size_t offset;

  offset = 0U;
  while (offset < length) {
    const char *line_end;
    char *end_ptr;
    unsigned long chunk_size;
    size_t chunk_begin;

    line_end = strstr(body + offset, "\r\n");
    if (line_end == NULL) {
      return 0;
    }
    chunk_size = strtoul(body + offset, &end_ptr, 16);
    if (end_ptr != line_end) {
      return 0;
    }
    offset = (size_t)(line_end - body) + 2U;
    if (chunk_size == 0UL) {
      return offset + 2U <= length && memcmp(body + offset, "\r\n", 2U) == 0;
    }
    chunk_begin = offset;
    if ((size_t)chunk_size > length - chunk_begin) {
      return 0;
    }
    offset = chunk_begin + (size_t)chunk_size;
    if (offset + 2U > length || memcmp(body + offset, "\r\n", 2U) != 0) {
      return 0;
    }
    offset += 2U;
  }
  return 0;
}

static int decode_chunked_request_body(const char *body, size_t length,
                                       char **out_body) {
  test_request_capture decoded;
  size_t offset;

  if (out_body == NULL) {
    return 0;
  }
  *out_body = NULL;
  memset(&decoded, 0, sizeof(decoded));
  offset = 0U;
  while (offset < length) {
    const char *line_end;
    char *end_ptr;
    unsigned long chunk_size;
    size_t chunk_begin;

    line_end = strstr(body + offset, "\r\n");
    if (line_end == NULL) {
      buffer_cleanup(&decoded);
      return 0;
    }
    chunk_size = strtoul(body + offset, &end_ptr, 16);
    if (end_ptr != line_end) {
      buffer_cleanup(&decoded);
      return 0;
    }
    offset = (size_t)(line_end - body) + 2U;
    if (chunk_size == 0UL) {
      if (offset + 2U > length || memcmp(body + offset, "\r\n", 2U) != 0) {
        buffer_cleanup(&decoded);
        return 0;
      }
      *out_body = decoded.data;
      decoded.data = NULL;
      return 1;
    }
    chunk_begin = offset;
    if ((size_t)chunk_size > length - chunk_begin) {
      buffer_cleanup(&decoded);
      return 0;
    }
    if (!buffer_append(&decoded, body + chunk_begin, (size_t)chunk_size)) {
      buffer_cleanup(&decoded);
      return 0;
    }
    offset = chunk_begin + (size_t)chunk_size;
    if (offset + 2U > length || memcmp(body + offset, "\r\n", 2U) != 0) {
      buffer_cleanup(&decoded);
      return 0;
    }
    offset += 2U;
  }
  buffer_cleanup(&decoded);
  return 0;
}

static int read_http_request(SSL *ssl, test_request_capture *capture) {
  char chunk[2048];
  const char *header_end;
  long content_length;
  size_t target_length;
  int chunked;

  memset(capture, 0, sizeof(*capture));
  header_end = NULL;
  content_length = 0L;
  target_length = 0U;
  chunked = 0;
  while (1) {
    int read_count;

    read_count = SSL_read(ssl, chunk, (int)sizeof(chunk));
    if (read_count <= 0) {
      return 0;
    }
    if (!buffer_append(capture, chunk, (size_t)read_count)) {
      return 0;
    }
    if (header_end == NULL) {
      header_end = find_header_end(capture->data, capture->length);
      if (header_end != NULL) {
        chunked = request_uses_chunked_transfer_encoding(capture->data);
        content_length = parse_content_length(capture->data);
        target_length = (size_t)((header_end + 4) - capture->data);
        if (chunked) {
          target_length = 0U;
        } else if (content_length > 0L) {
          target_length += (size_t)content_length;
        }
      }
    }
    if (header_end != NULL) {
      if (chunked) {
        if (chunked_request_complete(header_end + 4,
                                     capture->length -
                                         (size_t)((header_end + 4) -
                                                  capture->data))) {
          return 1;
        }
      } else if (capture->length >= target_length) {
        return 1;
      }
    }
  }
}

static const char *status_reason(int status) {
  switch (status) {
  case 200:
    return "OK";
  case 201:
    return "Created";
  case 204:
    return "No Content";
  case 400:
    return "Bad Request";
  case 500:
    return "Internal Server Error";
  default:
    return "OK";
  }
}

static int write_http_response(SSL *ssl, const https_expectation *expectation) {
  char response[16384];
  int written;
  size_t body_length;
  size_t offset;
  size_t index;

  body_length = expectation->response_body != NULL
                    ? strlen(expectation->response_body)
                    : 0U;
  written = snprintf(response, sizeof(response),
                     "HTTP/1.1 %d %s\r\n"
                     "Connection: close\r\n"
                     "Content-Length: %zu\r\n",
                     expectation->response_status,
                     status_reason(expectation->response_status), body_length);
  if (written < 0 || (size_t)written >= sizeof(response)) {
    return 0;
  }
  offset = (size_t)written;
  for (index = 0U; index < expectation->response_header_count; ++index) {
    written = snprintf(response + offset, sizeof(response) - offset, "%s\r\n",
                       expectation->response_headers[index]);
    if (written < 0 || (size_t)written >= sizeof(response) - offset) {
      return 0;
    }
    offset += (size_t)written;
  }
  if (offset + 2U > sizeof(response)) {
    return 0;
  }
  memcpy(response + offset, "\r\n", 2U);
  offset += 2U;
  if (SSL_write(ssl, response, (int)offset) != (int)offset) {
    return 0;
  }
  if (body_length > 0U) {
    return SSL_write(ssl, expectation->response_body, (int)body_length) ==
           (int)body_length;
  }
  return 1;
}

static int extract_common_name(X509 *cert, char *buffer, size_t buffer_size) {
  X509_NAME *subject;
  int len;

  subject = X509_get_subject_name(cert);
  if (subject == NULL) {
    return 0;
  }
  len = X509_NAME_get_text_by_NID(subject, NID_commonName, buffer,
                                  (int)buffer_size);
  return len >= 0;
}

static void verify_expectation(https_testserver *server, size_t index,
                               const https_expectation *expectation,
                               const test_request_capture *capture, SSL *ssl) {
  const char *request_line_end;
  const char *space;
  const char *path_end;
  const char *headers;
  const char *body;
  const char *body_view;
  char *decoded_body;
  size_t body_length;
  char method[32];
  char path[1024];
  size_t method_length;
  size_t path_length;
  size_t header_index;

  request_line_end = strstr(capture->data, "\r\n");
  if (request_line_end == NULL) {
    set_failure(server, "request missing request line");
    return;
  }
  space =
      memchr(capture->data, ' ', (size_t)(request_line_end - capture->data));
  if (space == NULL) {
    set_failure(server, "request line missing first space");
    return;
  }
  path_end = memchr(space + 1, ' ', (size_t)(request_line_end - (space + 1)));
  if (path_end == NULL) {
    set_failure(server, "request line missing second space");
    return;
  }

  method_length = (size_t)(space - capture->data);
  if (method_length >= sizeof(method)) {
    method_length = sizeof(method) - 1U;
  }
  memcpy(method, capture->data, method_length);
  method[method_length] = '\0';

  path_length = (size_t)(path_end - (space + 1));
  if (path_length >= sizeof(path)) {
    path_length = sizeof(path) - 1U;
  }
  memcpy(path, space + 1, path_length);
  path[path_length] = '\0';

  if (strcmp(method, expectation->method) != 0) {
    set_failure(server, "request[%zu] expected method %s, got %s", index,
                expectation->method, method);
    return;
  }
  if (strcmp(path, expectation->path) != 0) {
    set_failure(server, "request[%zu] expected path %s, got %s", index,
                expectation->path, path);
    return;
  }

  headers = request_line_end + 2;
  body = strstr(headers, "\r\n\r\n");
  if (body == NULL) {
    set_failure(server, "request missing header terminator");
    return;
  }
  body += 4;
  body_view = body;
  decoded_body = NULL;
  body_length = capture->length - (size_t)(body - capture->data);
  if (request_uses_chunked_transfer_encoding(headers)) {
    if (!decode_chunked_request_body(body, body_length, &decoded_body)) {
      set_failure(server, "failed to decode chunked request body");
      return;
    }
    body_view = decoded_body;
  }

  for (header_index = 0U; header_index < expectation->required_header_count;
       ++header_index) {
    if (strstr(headers, expectation->required_headers[header_index]) == NULL) {
      set_failure(server, "missing required header substring: %s",
                  expectation->required_headers[header_index]);
      free(decoded_body);
      return;
    }
  }
  if (expectation->expect_empty_body &&
      (body_view != NULL && body_view[0] != '\0')) {
    set_failure(server, "expected empty body, got %s",
                body_view != NULL ? body_view : "(null)");
    free(decoded_body);
    return;
  }
  for (header_index = 0U;
       header_index < expectation->required_body_substring_count;
       ++header_index) {
    if (strstr(body_view, expectation->required_body_substrings[header_index]) ==
        NULL) {
      set_failure(server, "missing required body substring: %s body=%s",
                  expectation->required_body_substrings[header_index],
                  body_view);
      free(decoded_body);
      return;
    }
  }
  free(decoded_body);
  if (expectation->expected_client_cn != NULL) {
    X509 *peer_cert;
    char common_name[256];

    peer_cert = SSL_get_peer_certificate(ssl);
    if (peer_cert == NULL) {
      set_failure(server, "server did not receive a client certificate");
      return;
    }
    memset(common_name, 0, sizeof(common_name));
    if (!extract_common_name(peer_cert, common_name, sizeof(common_name)) ||
        strcmp(common_name, expectation->expected_client_cn) != 0) {
      X509_free(peer_cert);
      set_failure(server, "expected client common name %s, got %s",
                  expectation->expected_client_cn, common_name);
      return;
    }
    X509_free(peer_cert);
  }
}

static void *https_testserver_main(void *context) {
  https_testserver *server;
  size_t index;

  server = (https_testserver *)context;
  for (index = 0U; index < server->expectation_count; ++index) {
    const https_expectation *expectation;
    struct sockaddr_in addr;
    socklen_t addr_len;
    int client_fd;
    SSL *ssl;
    test_request_capture capture;

    expectation = &server->expectations[index];
    addr_len = (socklen_t)sizeof(addr);
    client_fd =
        accept(server->listener_fd, (struct sockaddr *)&addr, &addr_len);
    if (client_fd < 0) {
      set_failure(server, "accept failed: %s", strerror(errno));
      break;
    }

    ssl = SSL_new(server->ssl_ctx);
    if (ssl == NULL) {
      close(client_fd);
      set_failure(server, "SSL_new failed");
      break;
    }
    SSL_set_fd(ssl, client_fd);
    if (SSL_accept(ssl) <= 0) {
      SSL_free(ssl);
      close(client_fd);
      set_failure(server, "SSL_accept failed");
      break;
    }

    if (!read_http_request(ssl, &capture)) {
      buffer_cleanup(&capture);
      SSL_free(ssl);
      close(client_fd);
      set_failure(server, "failed to read HTTP request");
      break;
    }
    verify_expectation(server, index, expectation, &capture, ssl);
    if (!write_http_response(ssl, expectation)) {
      set_failure(server, "failed to write HTTP response");
    }
    buffer_cleanup(&capture);
    SSL_free(ssl);
    close(client_fd);
    server->handled_count = index + 1U;
    if (server->failure_message[0] != '\0') {
      break;
    }
  }
  close(server->listener_fd);
  server->listener_fd = -1;
  return NULL;
}

static int add_extension(X509 *cert, X509 *issuer, int nid, const char *value) {
  X509_EXTENSION *extension;
  X509V3_CTX ctx;

  X509V3_set_ctx(&ctx, issuer, cert, NULL, NULL, 0);
  extension = X509V3_EXT_conf_nid(NULL, &ctx, nid, (char *)value);
  if (extension == NULL) {
    return 0;
  }
  if (X509_add_ext(cert, extension, -1) != 1) {
    X509_EXTENSION_free(extension);
    return 0;
  }
  X509_EXTENSION_free(extension);
  return 1;
}

static EVP_PKEY *generate_key(void) {
  EVP_PKEY_CTX *ctx;
  EVP_PKEY *key;

  ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
  key = NULL;
  if (ctx == NULL) {
    return NULL;
  }
  if (EVP_PKEY_keygen_init(ctx) <= 0 ||
      EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) <= 0 ||
      EVP_PKEY_keygen(ctx, &key) <= 0) {
    EVP_PKEY_CTX_free(ctx);
    return NULL;
  }
  EVP_PKEY_CTX_free(ctx);
  return key;
}

static X509 *generate_certificate(EVP_PKEY *subject_key,
                                  const char *common_name, X509 *issuer_cert,
                                  EVP_PKEY *issuer_key, int is_ca,
                                  const char *extended_usage,
                                  const char *subject_alt_name) {
  X509 *cert;
  X509_NAME *subject;

  cert = X509_new();
  if (cert == NULL) {
    return NULL;
  }
  if (X509_set_version(cert, 2) != 1 ||
      ASN1_INTEGER_set(X509_get_serialNumber(cert), next_test_serial()) != 1 ||
      X509_gmtime_adj(X509_getm_notBefore(cert), -3600L) == NULL ||
      X509_gmtime_adj(X509_getm_notAfter(cert), 86400L) == NULL ||
      X509_set_pubkey(cert, subject_key) != 1) {
    X509_free(cert);
    return NULL;
  }

  subject = X509_get_subject_name(cert);
  if (subject == NULL ||
      X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                 (const unsigned char *)common_name, -1, -1,
                                 0) != 1) {
    X509_free(cert);
    return NULL;
  }
  if (issuer_cert != NULL) {
    if (X509_set_issuer_name(cert, X509_get_subject_name(issuer_cert)) != 1) {
      X509_free(cert);
      return NULL;
    }
  } else if (X509_set_issuer_name(cert, subject) != 1) {
    X509_free(cert);
    return NULL;
  }

  if (!add_extension(cert, issuer_cert != NULL ? issuer_cert : cert,
                     NID_basic_constraints,
                     is_ca ? "critical,CA:TRUE" : "critical,CA:FALSE")) {
    X509_free(cert);
    return NULL;
  }
  if (!add_extension(cert, issuer_cert != NULL ? issuer_cert : cert,
                     NID_key_usage,
                     is_ca ? "critical,keyCertSign,cRLSign"
                           : "critical,digitalSignature,keyEncipherment")) {
    X509_free(cert);
    return NULL;
  }
  if (!is_ca && extended_usage != NULL &&
      !add_extension(cert, issuer_cert != NULL ? issuer_cert : cert,
                     NID_ext_key_usage, extended_usage)) {
    X509_free(cert);
    return NULL;
  }
  if (subject_alt_name != NULL &&
      !add_extension(cert, issuer_cert != NULL ? issuer_cert : cert,
                     NID_subject_alt_name, subject_alt_name)) {
    X509_free(cert);
    return NULL;
  }
  if (!add_extension(cert, issuer_cert != NULL ? issuer_cert : cert,
                     NID_subject_key_identifier, "hash")) {
    X509_free(cert);
    return NULL;
  }
  if (issuer_cert != NULL &&
      !add_extension(cert, issuer_cert, NID_authority_key_identifier,
                     "keyid:always")) {
    X509_free(cert);
    return NULL;
  }
  if (X509_sign(cert, issuer_key != NULL ? issuer_key : subject_key,
                EVP_sha256()) <= 0) {
    X509_free(cert);
    return NULL;
  }
  return cert;
}

static int write_bundle_file(const char *path, X509 *ca_cert, X509 *client_cert,
                             EVP_PKEY *client_key) {
  FILE *file;

  file = fopen(path, "wb");
  if (file == NULL) {
    return 0;
  }
  if (ca_cert != NULL && PEM_write_X509(file, ca_cert) != 1) {
    fclose(file);
    return 0;
  }
  if (client_cert != NULL && PEM_write_X509(file, client_cert) != 1) {
    fclose(file);
    return 0;
  }
  if (client_key != NULL &&
      PEM_write_PrivateKey(file, client_key, NULL, NULL, 0, NULL, NULL) != 1) {
    fclose(file);
    return 0;
  }
  fclose(file);
  return 1;
}

static int https_tls_material_init(https_tls_material *material,
                                   int include_ca_in_bundle) {
  char template_path[] = "/tmp/liblockdc-transport-XXXXXX";
  EVP_PKEY *client_key;
  X509 *client_cert;

  memset(material, 0, sizeof(*material));
  if (mkdtemp(template_path) == NULL) {
    return 0;
  }
  if (snprintf(material->temp_dir, sizeof(material->temp_dir), "%s",
               template_path) >= (int)sizeof(material->temp_dir) ||
      snprintf(material->client_bundle_path,
               sizeof(material->client_bundle_path), "%s/client-bundle.pem",
               template_path) >= (int)sizeof(material->client_bundle_path)) {
    return 0;
  }

  material->ca_key = generate_key();
  material->server_key = generate_key();
  client_key = generate_key();
  if (material->ca_key == NULL || material->server_key == NULL ||
      client_key == NULL) {
    EVP_PKEY_free(client_key);
    return 0;
  }

  material->ca_cert = generate_certificate(
      material->ca_key, "liblockdc test ca", NULL, NULL, 1, NULL, NULL);
  material->server_cert =
      generate_certificate(material->server_key, "127.0.0.1", material->ca_cert,
                           material->ca_key, 0, "serverAuth", "IP:127.0.0.1");
  client_cert = generate_certificate(client_key, "liblockdc test client",
                                     material->ca_cert, material->ca_key, 0,
                                     "clientAuth", NULL);
  if (material->ca_cert == NULL || material->server_cert == NULL ||
      client_cert == NULL) {
    X509_free(client_cert);
    EVP_PKEY_free(client_key);
    return 0;
  }

  if (!write_bundle_file(material->client_bundle_path,
                         include_ca_in_bundle ? material->ca_cert : NULL,
                         client_cert, client_key)) {
    X509_free(client_cert);
    EVP_PKEY_free(client_key);
    return 0;
  }

  X509_free(client_cert);
  EVP_PKEY_free(client_key);
  return 1;
}

static void unlink_if_exists(const char *path) {
  if (path[0] != '\0') {
    unlink(path);
  }
}

static void https_tls_material_cleanup(https_tls_material *material) {
  unlink_if_exists(material->client_bundle_path);
  if (material->temp_dir[0] != '\0') {
    rmdir(material->temp_dir);
  }
  X509_free(material->ca_cert);
  EVP_PKEY_free(material->ca_key);
  X509_free(material->server_cert);
  EVP_PKEY_free(material->server_key);
  memset(material, 0, sizeof(*material));
}

static int https_testserver_start(https_testserver *server,
                                  const https_tls_material *material,
                                  const https_expectation *expectations,
                                  size_t expectation_count) {
  struct sockaddr_in addr;
  socklen_t addr_len;
  int fd;

  memset(server, 0, sizeof(*server));
  server->listener_fd = -1;
  server->expectations = expectations;
  server->expectation_count = expectation_count;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }
  {
    int yes;

    yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  }

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
      listen(fd, (int)expectation_count) != 0) {
    close(fd);
    return 0;
  }
  addr_len = (socklen_t)sizeof(addr);
  if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) != 0) {
    close(fd);
    return 0;
  }

  server->ssl_ctx = SSL_CTX_new(TLS_server_method());
  if (server->ssl_ctx == NULL) {
    close(fd);
    return 0;
  }
  if (SSL_CTX_use_certificate(server->ssl_ctx, material->server_cert) != 1 ||
      SSL_CTX_use_PrivateKey(server->ssl_ctx, material->server_key) != 1 ||
      SSL_CTX_check_private_key(server->ssl_ctx) != 1) {
    SSL_CTX_free(server->ssl_ctx);
    close(fd);
    return 0;
  }
  if (X509_STORE_add_cert(SSL_CTX_get_cert_store(server->ssl_ctx),
                          material->ca_cert) != 1) {
    ERR_clear_error();
  }
  SSL_CTX_set_verify(server->ssl_ctx,
                     SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);

  server->listener_fd = fd;
  server->port = ntohs(addr.sin_port);
  if (pthread_create(&server->thread, NULL, https_testserver_main, server) !=
      0) {
    SSL_CTX_free(server->ssl_ctx);
    close(fd);
    server->listener_fd = -1;
    return 0;
  }
  return 1;
}

static void https_testserver_stop(https_testserver *server) {
  if (server->listener_fd >= 0 || server->expectation_count > 0U) {
    pthread_join(server->thread, NULL);
  }
  if (server->ssl_ctx != NULL) {
    SSL_CTX_free(server->ssl_ctx);
  }
}

static void init_client_config(lc_engine_client_config *config,
                               unsigned short port, const char *bundle_path) {
  static char endpoint[128];
  static const char *endpoints[1];

  lc_engine_client_config_init(config);
  snprintf(endpoint, sizeof(endpoint), "https://127.0.0.1:%u", (unsigned)port);
  endpoints[0] = endpoint;
  config->endpoints = endpoints;
  config->endpoint_count = 1U;
  config->client_bundle_path = bundle_path;
  config->default_namespace = "transport-ns";
  config->prefer_http_2 = 0;
}

static void init_client_config_two_endpoints(lc_engine_client_config *config,
                                             unsigned short port,
                                             const char *bundle_path) {
  static char endpoint_a[128];
  static char endpoint_b[128];
  static const char *endpoints[2];

  lc_engine_client_config_init(config);
  snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
           (unsigned)port);
  snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
           (unsigned)port);
  endpoints[0] = endpoint_a;
  endpoints[1] = endpoint_b;
  config->endpoints = endpoints;
  config->endpoint_count = 2U;
  config->client_bundle_path = bundle_path;
  config->default_namespace = "transport-ns";
  config->prefer_http_2 = 0;
}

static void init_public_client_config(lc_client_config *config,
                                      unsigned short port,
                                      const char *bundle_path,
                                      pslog_logger *logger) {
  static char endpoint[128];
  static const char *endpoints[1];

  lc_client_config_init(config);
  snprintf(endpoint, sizeof(endpoint), "https://127.0.0.1:%u", (unsigned)port);
  endpoints[0] = endpoint;
  config->endpoints = endpoints;
  config->endpoint_count = 1U;
  config->client_bundle_path = bundle_path;
  config->default_namespace = "transport-ns";
  config->prefer_http_2 = 0;
  config->logger = logger;
}

static pslog_logger *open_test_logger(FILE **out_fp) {
  FILE *fp;
  pslog_config config;

  fp = tmpfile();
  if (fp == NULL) {
    return NULL;
  }
  pslog_default_config(&config);
  config.mode = PSLOG_MODE_JSON;
  config.min_level = PSLOG_LEVEL_TRACE;
  config.timestamps = 0;
  config.verbose_fields = 1;
  config.output = pslog_output_from_fp(fp, 0);
  *out_fp = fp;
  return pslog_new(&config);
}

static char *read_stream_text(FILE *fp) {
  long length;
  char *buffer;

  if (fp == NULL || fflush(fp) != 0 || fseek(fp, 0L, SEEK_END) != 0) {
    return NULL;
  }
  length = ftell(fp);
  if (length < 0L || fseek(fp, 0L, SEEK_SET) != 0) {
    return NULL;
  }
  buffer = (char *)calloc((size_t)length + 1U, 1U);
  if (buffer == NULL) {
    return NULL;
  }
  if (length > 0L && fread(buffer, 1U, (size_t)length, fp) != (size_t)length) {
    free(buffer);
    return NULL;
  }
  buffer[length] = '\0';
  return buffer;
}

static void assert_server_ok(https_testserver *server) {
  assert_int_equal(server->handled_count, server->expectation_count);
  if (server->failure_message[0] != '\0') {
    fail_msg("%s", server->failure_message);
  }
  assert_true(server->failure_message[0] == '\0');
}

static int capture_delivery_begin(void *context,
                                  const lc_engine_dequeue_response *delivery,
                                  lc_engine_error *error) {
  subscribe_capture *capture;

  (void)error;
  capture = (subscribe_capture *)context;
  capture->begin_calls += 1;
  if (delivery->message_id != NULL) {
    snprintf(capture->message_id, sizeof(capture->message_id), "%s",
             delivery->message_id);
  }
  return 1;
}

static int capture_delivery_chunk(void *context, const void *bytes,
                                  size_t count, lc_engine_error *error) {
  subscribe_capture *capture;
  size_t copy_count;

  (void)error;
  capture = (subscribe_capture *)context;
  copy_count = count;
  if (copy_count > sizeof(capture->payload) - capture->payload_length - 1U) {
    copy_count = sizeof(capture->payload) - capture->payload_length - 1U;
  }
  memcpy(capture->payload + capture->payload_length, bytes, copy_count);
  capture->payload_length += copy_count;
  capture->payload[capture->payload_length] = '\0';
  return 1;
}

static int capture_delivery_end(void *context,
                                const lc_engine_dequeue_response *delivery,
                                lc_engine_error *error) {
  subscribe_capture *capture;

  (void)delivery;
  (void)error;
  capture = (subscribe_capture *)context;
  capture->end_calls += 1;
  return 1;
}

static void test_client_open_rejects_bundle_without_ca(void **state) {
  https_tls_material material;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 0));
  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  lc_engine_client_config_init(&config);
  {
    static const char *endpoints[] = {"https://127.0.0.1:1"};

    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.client_bundle_path = material.client_bundle_path;
  }
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_null(client);
  assert_non_null(error.message);
  assert_non_null(
      strstr(error.message, "client bundle does not contain a CA certificate"));
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_state_transport_paths_use_mtls(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-State-ETag: etag-1", "X-If-Version: 4"};
  static const char *mutate_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-Version: 5"};
  static const char *metadata_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-Version: 6"};
  static const char *remove_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-State-ETag: etag-4", "X-If-Version: 7"};
  static const char *keepalive_headers[] = {"Content-Type: application/json",
                                            "X-Fencing-Token: 11"};
  static const char *release_headers[] = {"Content-Type: application/json",
                                          "X-Fencing-Token: 11"};
  static const char *acquire_body[] = {"\"namespace\":\"transport-ns\"",
                                       "\"key\":\"resource/1\"",
                                       "\"ttl_seconds\":30",
                                       "\"owner\":\"owner-a\"",
                                       "\"block_seconds\":5",
                                       "\"if_not_exists\":true",
                                       "\"txn_id\":\"txn-acquire\""};
  static const char *mutate_body[] = {"\"namespace\":\"transport-ns\"",
                                      "\"mutations\":[\"set($.value,2)\","
                                      "\"set($.ready,true)\"]"};
  static const char *metadata_body[] = {"\"namespace\":\"transport-ns\"",
                                        "\"query_hidden\":false"};
  static const char *keepalive_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"lease_id\":\"lease-1\"", "\"ttl_seconds\":45",
      "\"txn_id\":\"txn-acquire\""};
  static const char *release_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"lease_id\":\"lease-1\"", "\"txn_id\":\"txn-acquire\"",
      "\"rollback\":true"};
  static const char *query_body[] = {"\"namespace\":\"transport-ns\"",
                                     "\"selector\":{\"owner\":\"owner-a\"}",
                                     "\"limit\":5",
                                     "\"cursor\":\"cursor-0\"",
                                     "\"fields\":[\"key\"]",
                                     "\"return\":\"compact\""};
  static const char *corr_acquire[] = {"X-Correlation-Id: corr-acquire",
                                       "Content-Type: application/json"};
  static const char *corr_describe[] = {"X-Correlation-Id: corr-describe",
                                        "Content-Type: application/json"};
  static const char *corr_get[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-2", "X-Key-Version: 5", "X-Fencing-Token: 11"};
  static const char *corr_update[] = {"X-Correlation-Id: corr-update",
                                      "Content-Type: application/json"};
  static const char *corr_mutate[] = {"X-Correlation-Id: corr-mutate",
                                      "Content-Type: application/json"};
  static const char *corr_metadata[] = {"X-Correlation-Id: corr-metadata",
                                        "Content-Type: application/json"};
  static const char *corr_remove[] = {"X-Correlation-Id: corr-remove",
                                      "Content-Type: application/json"};
  static const char *corr_keepalive[] = {"X-Correlation-Id: corr-keepalive",
                                         "Content-Type: application/json"};
  static const char *corr_release[] = {"X-Correlation-Id: corr-release",
                                       "Content-Type: application/json"};
  static const char *corr_query[] = {"X-Correlation-Id: corr-query",
                                     "Content-Type: application/json"};
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body,
       sizeof(acquire_body) / sizeof(acquire_body[0]), 0, 200, corr_acquire,
       sizeof(corr_acquire) / sizeof(corr_acquire[0]),
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"GET", "/v1/describe?key=resource%2F1&namespace=transport-ns", NULL, 0U,
       NULL, 0U, 1, 200, corr_describe,
       sizeof(corr_describe) / sizeof(corr_describe[0]),
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"expires_at_unix\":1000,\"version\":4,\"state_etag\":\"etag-1\","
       "\"updated_at_unix\":2000,\"metadata\":{\"query_hidden\":true}}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, corr_get, sizeof(corr_get) / sizeof(corr_get[0]),
       "{\"value\":1}", "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns",
       update_headers, sizeof(update_headers) / sizeof(update_headers[0]),
       (const char *const[]){"{\"value\":2}"}, 1U, 0, 200, corr_update,
       sizeof(corr_update) / sizeof(corr_update[0]),
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\",\"bytes\":11}",
       "liblockdc test client"},
      {"POST", "/v1/mutate?key=resource%2F1&namespace=transport-ns",
       mutate_headers, sizeof(mutate_headers) / sizeof(mutate_headers[0]),
       mutate_body, sizeof(mutate_body) / sizeof(mutate_body[0]), 0, 200,
       corr_mutate, sizeof(corr_mutate) / sizeof(corr_mutate[0]),
       "{\"new_version\":6,\"new_state_etag\":\"etag-3\",\"bytes\":19}",
       "liblockdc test client"},
      {"POST", "/v1/metadata?key=resource%2F1&namespace=transport-ns",
       metadata_headers, sizeof(metadata_headers) / sizeof(metadata_headers[0]),
       metadata_body, sizeof(metadata_body) / sizeof(metadata_body[0]), 0, 200,
       corr_metadata, sizeof(corr_metadata) / sizeof(corr_metadata[0]),
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"version\":7,\"metadata\":{\"query_hidden\":false}}",
       "liblockdc test client"},
      {"POST", "/v1/remove?key=resource%2F1&namespace=transport-ns",
       remove_headers, sizeof(remove_headers) / sizeof(remove_headers[0]), NULL,
       0U, 1, 200, corr_remove, sizeof(corr_remove) / sizeof(corr_remove[0]),
       "{\"removed\":true,\"new_version\":8}", "liblockdc test client"},
      {"POST", "/v1/keepalive", keepalive_headers,
       sizeof(keepalive_headers) / sizeof(keepalive_headers[0]), keepalive_body,
       sizeof(keepalive_body) / sizeof(keepalive_body[0]), 0, 200,
       corr_keepalive, sizeof(corr_keepalive) / sizeof(corr_keepalive[0]),
       "{\"lease_expires_at_unix\":5000,\"version\":9,"
       "\"state_etag\":\"etag-9\"}",
       "liblockdc test client"},
      {"POST", "/v1/release", release_headers,
       sizeof(release_headers) / sizeof(release_headers[0]), release_body,
       sizeof(release_body) / sizeof(release_body[0]), 0, 200, corr_release,
       sizeof(corr_release) / sizeof(corr_release[0]), "{\"released\":true}",
       "liblockdc test client"},
      {"POST", "/v1/query", json_header, 1U, query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, corr_query,
       sizeof(corr_query) / sizeof(corr_query[0]),
       "{\"items\":[{\"key\":\"resource/1\"}],\"cursor\":\"cursor-1\","
       "\"index_seq\":12}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_acquire_request acquire_req;
  lc_engine_acquire_response acquire_res;
  lc_engine_describe_request describe_req;
  lc_engine_describe_response describe_res;
  lc_engine_get_request get_req;
  lc_engine_get_response get_res;
  lc_engine_update_request update_req;
  lc_engine_update_response update_res;
  lc_engine_mutate_request mutate_req;
  lc_engine_mutate_response mutate_res;
  lc_engine_metadata_request metadata_req;
  lc_engine_metadata_response metadata_res;
  lc_engine_remove_request remove_req;
  lc_engine_remove_response remove_res;
  lc_engine_keepalive_request keepalive_req;
  lc_engine_keepalive_response keepalive_res;
  lc_engine_release_request release_req;
  lc_engine_release_response release_res;
  lc_engine_query_request query_req;
  lc_engine_query_response query_res;
  static const char *mutations[] = {"set($.value,2)", "set($.ready,true)"};
  const char update_body[] = "{\"value\":2}";
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(client);

  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&acquire_res, 0, sizeof(acquire_res));
  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  acquire_req.block_seconds = 5L;
  acquire_req.if_not_exists = 1;
  acquire_req.txn_id = "txn-acquire";
  rc = lc_engine_client_acquire(client, &acquire_req, &acquire_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(acquire_res.correlation_id, "corr-acquire");
  assert_int_equal(acquire_res.fencing_token, 11L);

  memset(&describe_req, 0, sizeof(describe_req));
  memset(&describe_res, 0, sizeof(describe_res));
  describe_req.namespace_name = "transport-ns";
  describe_req.key = "resource/1";
  rc = lc_engine_client_describe(client, &describe_req, &describe_res, &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg("describe rc=%d code=%d http_status=%ld message=%s detail=%s "
             "server=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(none)");
  }
  assert_true(describe_res.has_query_hidden);
  assert_true(describe_res.query_hidden);

  memset(&get_req, 0, sizeof(get_req));
  memset(&get_res, 0, sizeof(get_res));
  get_req.namespace_name = "transport-ns";
  get_req.key = "resource/1";
  get_req.public_read = 1;
  rc = lc_engine_client_get(client, &get_req, &get_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_memory_equal(get_res.body, "{\"value\":1}", strlen("{\"value\":1}"));
  assert_string_equal(get_res.etag, "etag-2");
  assert_int_equal(get_res.version, 5L);
  assert_int_equal(get_res.fencing_token, 11L);

  memset(&update_req, 0, sizeof(update_req));
  memset(&update_res, 0, sizeof(update_res));
  update_req.namespace_name = "transport-ns";
  update_req.key = "resource/1";
  update_req.txn_id = "txn-acquire";
  update_req.fencing_token = 11L;
  update_req.if_state_etag = "etag-1";
  update_req.if_version = 4L;
  update_req.has_if_version = 1;
  update_req.body = update_body;
  update_req.body_length = strlen(update_body);
  update_req.content_type = "application/json";
  rc = lc_engine_client_update(client, &update_req, &update_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(update_res.new_state_etag, "etag-2");

  memset(&mutate_req, 0, sizeof(mutate_req));
  memset(&mutate_res, 0, sizeof(mutate_res));
  mutate_req.namespace_name = "transport-ns";
  mutate_req.key = "resource/1";
  mutate_req.txn_id = "txn-acquire";
  mutate_req.fencing_token = 11L;
  mutate_req.if_state_etag = "etag-2";
  mutate_req.if_version = 5L;
  mutate_req.has_if_version = 1;
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 2U;
  rc = lc_engine_client_mutate(client, &mutate_req, &mutate_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(mutate_res.new_version, 6L);

  memset(&metadata_req, 0, sizeof(metadata_req));
  memset(&metadata_res, 0, sizeof(metadata_res));
  metadata_req.namespace_name = "transport-ns";
  metadata_req.key = "resource/1";
  metadata_req.txn_id = "txn-acquire";
  metadata_req.fencing_token = 11L;
  metadata_req.if_version = 6L;
  metadata_req.has_if_version = 1;
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 0;
  rc = lc_engine_client_update_metadata(client, &metadata_req, &metadata_res,
                                        &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(metadata_res.has_query_hidden);
  assert_false(metadata_res.query_hidden);

  memset(&remove_req, 0, sizeof(remove_req));
  memset(&remove_res, 0, sizeof(remove_res));
  remove_req.namespace_name = "transport-ns";
  remove_req.key = "resource/1";
  remove_req.txn_id = "txn-acquire";
  remove_req.fencing_token = 11L;
  remove_req.if_state_etag = "etag-4";
  remove_req.if_version = 7L;
  remove_req.has_if_version = 1;
  rc = lc_engine_client_remove(client, &remove_req, &remove_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(remove_res.removed);

  memset(&keepalive_req, 0, sizeof(keepalive_req));
  memset(&keepalive_res, 0, sizeof(keepalive_res));
  keepalive_req.namespace_name = "transport-ns";
  keepalive_req.key = "resource/1";
  keepalive_req.lease_id = "lease-1";
  keepalive_req.txn_id = "txn-acquire";
  keepalive_req.ttl_seconds = 45L;
  keepalive_req.fencing_token = 11L;
  rc = lc_engine_client_keepalive(client, &keepalive_req, &keepalive_res,
                                  &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg("keepalive rc=%d code=%d http_status=%ld message=%s server=%s "
             "detail=%s failure=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.server_error_code != NULL ? error.server_error_code
                                             : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message);
  }
  assert_int_equal(keepalive_res.version, 9L);

  memset(&release_req, 0, sizeof(release_req));
  memset(&release_res, 0, sizeof(release_res));
  release_req.namespace_name = "transport-ns";
  release_req.key = "resource/1";
  release_req.lease_id = "lease-1";
  release_req.txn_id = "txn-acquire";
  release_req.fencing_token = 11L;
  release_req.rollback = 1;
  rc = lc_engine_client_release(client, &release_req, &release_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(release_res.released);

  memset(&query_req, 0, sizeof(query_req));
  memset(&query_res, 0, sizeof(query_res));
  query_req.selector_json = "{\"owner\":\"owner-a\"}";
  query_req.limit = 5L;
  query_req.cursor = "cursor-0";
  query_req.fields_json = "[\"key\"]";
  query_req.return_mode = "compact";
  rc = lc_engine_client_query(client, &query_req, &query_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(query_res.cursor, "cursor-1");
  assert_int_equal(query_res.index_seq, 12UL);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);

  lc_engine_acquire_response_cleanup(&acquire_res);
  lc_engine_describe_response_cleanup(&describe_res);
  lc_engine_get_response_cleanup(&get_res);
  lc_engine_update_response_cleanup(&update_res);
  lc_engine_mutate_response_cleanup(&mutate_res);
  lc_engine_metadata_response_cleanup(&metadata_res);
  lc_engine_remove_response_cleanup(&remove_res);
  lc_engine_keepalive_response_cleanup(&keepalive_res);
  lc_engine_release_response_cleanup(&release_res);
  lc_engine_query_response_cleanup(&query_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_management_transport_paths_use_mtls(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  https_expectation expectations[] = {
      {"GET", "/v1/namespace?namespace=team-a", NULL, 0U, NULL, 0U, 1, 200,
       (const char *const[]){"X-Correlation-Id: corr-ns-get",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"team-a\",\"query\":{\"preferred_engine\":\"index\","
       "\"fallback_engine\":\"scan\"}}",
       "liblockdc test client"},
      {"PUT", "/v1/namespace", json_header, 1U,
       (const char *const[]){"\"namespace\":\"team-a\"",
                             "\"preferred_engine\":\"index\"",
                             "\"fallback_engine\":\"scan\""},
       3U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-ns-put",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"team-a\",\"query\":{\"preferred_engine\":\"index\","
       "\"fallback_engine\":\"scan\"}}",
       "liblockdc test client"},
      {"POST", "/v1/index/flush", json_header, 1U,
       (const char *const[]){"\"namespace\":\"team-a\"", "\"mode\":\"full\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-flush",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"team-a\",\"mode\":\"full\",\"flush_id\":\"flush-1\","
       "\"accepted\":true,\"flushed\":true,\"index_seq\":99}",
       "liblockdc test client"},
      {"POST", "/v1/txn/replay", json_header, 1U,
       (const char *const[]){"\"txn_id\":\"txn-77\""}, 1U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-replay",
                             "Content-Type: application/json"},
       2U, "{\"txn_id\":\"txn-77\",\"state\":\"prepared\"}",
       "liblockdc test client"},
      {"POST", "/v1/txn/decide", json_header, 1U,
       (const char *const[]){"\"txn_id\":\"txn-77\"", "\"state\":\"prepare\"",
                             "\"participants\":[{\"namespace\":\"team-a\","
                             "\"key\":\"k1\",\"backend_hash\":\"bh-1\"}]",
                             "\"expires_at_unix\":12345", "\"tc_term\":9",
                             "\"target_backend_hash\":\"bh-1\""},
       5U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-decide",
                             "Content-Type: application/json"},
       2U, "{\"txn_id\":\"txn-77\",\"state\":\"prepare\"}",
       "liblockdc test client"},
      {"POST", "/v1/txn/commit", json_header, 1U,
       (const char *const[]){"\"txn_id\":\"txn-77\"", "\"state\":\"commit\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-commit",
                             "Content-Type: application/json"},
       2U, "{\"txn_id\":\"txn-77\",\"state\":\"commit\"}",
       "liblockdc test client"},
      {"POST", "/v1/txn/rollback", json_header, 1U,
       (const char *const[]){"\"txn_id\":\"txn-77\"", "\"state\":\"rollback\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-rollback",
                             "Content-Type: application/json"},
       2U, "{\"txn_id\":\"txn-77\",\"state\":\"rollback\"}",
       "liblockdc test client"},
      {"POST", "/v1/tc/lease/acquire", json_header, 1U,
       (const char *const[]){"\"candidate_id\":\"node-a\"",
                             "\"candidate_endpoint\":\"https://node-a:9443\"",
                             "\"term\":3", "\"ttl_ms\":2000"},
       4U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-tc-acquire",
                             "Content-Type: application/json"},
       2U,
       "{\"granted\":true,\"leader_id\":\"node-a\","
       "\"leader_endpoint\":\"https://node-a:9443\",\"term\":3,"
       "\"expires_at\":9000}",
       "liblockdc test client"},
      {"POST", "/v1/tc/lease/renew", json_header, 1U,
       (const char *const[]){"\"leader_id\":\"node-a\"", "\"term\":3",
                             "\"ttl_ms\":2000"},
       3U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-tc-renew",
                             "Content-Type: application/json"},
       2U,
       "{\"granted\":true,\"leader_id\":\"node-a\","
       "\"leader_endpoint\":\"https://node-a:9443\",\"term\":3,"
       "\"expires_at\":9100}",
       "liblockdc test client"},
      {"POST", "/v1/tc/lease/release", json_header, 1U,
       (const char *const[]){"\"leader_id\":\"node-a\"", "\"term\":3"}, 2U, 0,
       200,
       (const char *const[]){"X-Correlation-Id: corr-tc-release",
                             "Content-Type: application/json"},
       2U, "{\"released\":true}", "liblockdc test client"},
      {"GET", "/v1/tc/leader", NULL, 0U, NULL, 0U, 1, 200,
       (const char *const[]){"X-Correlation-Id: corr-tc-leader",
                             "Content-Type: application/json"},
       2U,
       "{\"granted\":true,\"leader_id\":\"node-a\","
       "\"leader_endpoint\":\"https://node-a:9443\",\"term\":3,"
       "\"expires_at\":9200}",
       "liblockdc test client"},
      {"POST", "/v1/tc/cluster/announce", json_header, 1U,
       (const char *const[]){"\"self_endpoint\":\"https://node-a:9443\""}, 1U,
       0, 200,
       (const char *const[]){"X-Correlation-Id: corr-cluster-announce",
                             "Content-Type: application/json"},
       2U,
       "{\"endpoints\":[\"https://node-a:9443\",\"https://node-b:9443\"],"
       "\"updated_at_unix\":100,\"expires_at_unix\":200}",
       "liblockdc test client"},
      {"POST", "/v1/tc/cluster/leave", NULL, 0U, NULL, 0U, 1, 200,
       (const char *const[]){"X-Correlation-Id: corr-cluster-leave",
                             "Content-Type: application/json"},
       2U,
       "{\"endpoints\":[\"https://node-b:9443\"],\"updated_at_unix\":101,"
       "\"expires_at_unix\":201}",
       "liblockdc test client"},
      {"GET", "/v1/tc/cluster/list", NULL, 0U, NULL, 0U, 1, 200,
       (const char *const[]){"X-Correlation-Id: corr-cluster-list",
                             "Content-Type: application/json"},
       2U,
       "{\"endpoints\":[\"https://node-b:9443\"],\"updated_at_unix\":102,"
       "\"expires_at_unix\":202}",
       "liblockdc test client"},
      {"POST", "/v1/tc/rm/register", json_header, 1U,
       (const char *const[]){"\"backend_hash\":\"bh-1\"",
                             "\"endpoint\":\"https://rm-a:9443\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-rm-register",
                             "Content-Type: application/json"},
       2U,
       "{\"backend_hash\":\"bh-1\",\"endpoints\":[\"https://rm-a:9443\"],"
       "\"updated_at_unix\":300}",
       "liblockdc test client"},
      {"POST", "/v1/tc/rm/unregister", json_header, 1U,
       (const char *const[]){"\"backend_hash\":\"bh-1\"",
                             "\"endpoint\":\"https://rm-a:9443\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-rm-unregister",
                             "Content-Type: application/json"},
       2U,
       "{\"backend_hash\":\"bh-1\",\"endpoints\":[],\"updated_at_unix\":301}",
       "liblockdc test client"},
      {"GET", "/v1/tc/rm/list", NULL, 0U, NULL, 0U, 1, 200,
       (const char *const[]){"X-Correlation-Id: corr-rm-list",
                             "Content-Type: application/json"},
       2U,
       "{\"backends\":[{\"backend_hash\":\"bh-1\","
       "\"endpoints\":[\"https://rm-b:9443\"],\"updated_at_unix\":302}],"
       "\"updated_at_unix\":303}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_namespace_config_request ns_req;
  lc_engine_namespace_config_response ns_res;
  lc_engine_index_flush_request flush_req;
  lc_engine_index_flush_response flush_res;
  lc_engine_txn_replay_request replay_req;
  lc_engine_txn_replay_response replay_res;
  lc_engine_txn_participant participant;
  lc_engine_txn_decision_request decision_req;
  lc_engine_txn_decision_response decision_res;
  lc_engine_tc_lease_acquire_request tc_acquire_req;
  lc_engine_tc_lease_acquire_response tc_acquire_res;
  lc_engine_tc_lease_renew_request tc_renew_req;
  lc_engine_tc_lease_renew_response tc_renew_res;
  lc_engine_tc_lease_release_request tc_release_req;
  lc_engine_tc_lease_release_response tc_release_res;
  lc_engine_tc_leader_response tc_leader_res;
  lc_engine_tc_cluster_announce_request cluster_req;
  lc_engine_tc_cluster_response cluster_res;
  lc_engine_tcrm_register_request rm_req;
  lc_engine_tcrm_register_response rm_res;
  lc_engine_tcrm_unregister_request rm_unreg_req;
  lc_engine_tcrm_unregister_response rm_unreg_res;
  lc_engine_tcrm_list_response rm_list_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&ns_req, 0, sizeof(ns_req));
  memset(&ns_res, 0, sizeof(ns_res));
  ns_req.namespace_name = "team-a";
  ns_req.preferred_engine = "index";
  ns_req.fallback_engine = "scan";
  rc = lc_engine_client_get_namespace_config(client, ns_req.namespace_name,
                                             &ns_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(ns_res.preferred_engine, "index");

  lc_engine_namespace_config_response_cleanup(&ns_res);
  rc = lc_engine_client_update_namespace_config(client, &ns_req, &ns_res,
                                                &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&flush_req, 0, sizeof(flush_req));
  memset(&flush_res, 0, sizeof(flush_res));
  flush_req.namespace_name = "team-a";
  flush_req.mode = "full";
  rc = lc_engine_client_index_flush(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(flush_res.accepted);

  memset(&replay_req, 0, sizeof(replay_req));
  memset(&replay_res, 0, sizeof(replay_res));
  replay_req.txn_id = "txn-77";
  rc = lc_engine_client_txn_replay(client, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&participant, 0, sizeof(participant));
  participant.namespace_name = "team-a";
  participant.key = "k1";
  participant.backend_hash = "bh-1";
  memset(&decision_req, 0, sizeof(decision_req));
  memset(&decision_res, 0, sizeof(decision_res));
  decision_req.txn_id = "txn-77";
  decision_req.state = "prepare";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  decision_req.expires_at_unix = 12345L;
  decision_req.tc_term = 9UL;
  decision_req.target_backend_hash = "bh-1";
  rc =
      lc_engine_client_txn_decide(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  lc_engine_txn_decision_response_cleanup(&decision_res);
  rc =
      lc_engine_client_txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  lc_engine_txn_decision_response_cleanup(&decision_res);
  rc = lc_engine_client_txn_rollback(client, &decision_req, &decision_res,
                                     &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&tc_acquire_req, 0, sizeof(tc_acquire_req));
  memset(&tc_acquire_res, 0, sizeof(tc_acquire_res));
  tc_acquire_req.candidate_id = "node-a";
  tc_acquire_req.candidate_endpoint = "https://node-a:9443";
  tc_acquire_req.term = 3UL;
  tc_acquire_req.ttl_ms = 2000L;
  rc = lc_engine_client_tc_lease_acquire(client, &tc_acquire_req,
                                         &tc_acquire_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(tc_acquire_res.granted);

  memset(&tc_renew_req, 0, sizeof(tc_renew_req));
  memset(&tc_renew_res, 0, sizeof(tc_renew_res));
  tc_renew_req.leader_id = "node-a";
  tc_renew_req.term = 3UL;
  tc_renew_req.ttl_ms = 2000L;
  rc = lc_engine_client_tc_lease_renew(client, &tc_renew_req, &tc_renew_res,
                                       &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(tc_renew_res.renewed);

  memset(&tc_release_req, 0, sizeof(tc_release_req));
  memset(&tc_release_res, 0, sizeof(tc_release_res));
  tc_release_req.leader_id = "node-a";
  tc_release_req.term = 3UL;
  rc = lc_engine_client_tc_lease_release(client, &tc_release_req,
                                         &tc_release_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(tc_release_res.released);

  memset(&tc_leader_res, 0, sizeof(tc_leader_res));
  rc = lc_engine_client_tc_leader(client, &tc_leader_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(tc_leader_res.leader_id, "node-a");

  memset(&cluster_req, 0, sizeof(cluster_req));
  memset(&cluster_res, 0, sizeof(cluster_res));
  cluster_req.self_endpoint = "https://node-a:9443";
  rc = lc_engine_client_tc_cluster_announce(client, &cluster_req, &cluster_res,
                                            &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(cluster_res.endpoints.count, 2U);

  lc_engine_tc_cluster_response_cleanup(&cluster_res);
  rc = lc_engine_client_tc_cluster_leave(client, &cluster_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  lc_engine_tc_cluster_response_cleanup(&cluster_res);
  rc = lc_engine_client_tc_cluster_list(client, &cluster_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&rm_req, 0, sizeof(rm_req));
  memset(&rm_res, 0, sizeof(rm_res));
  rm_req.backend_hash = "bh-1";
  rm_req.endpoint = "https://rm-a:9443";
  rc = lc_engine_client_tcrm_register(client, &rm_req, &rm_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(rm_res.backend_hash, "bh-1");

  memset(&rm_unreg_req, 0, sizeof(rm_unreg_req));
  memset(&rm_unreg_res, 0, sizeof(rm_unreg_res));
  rm_unreg_req.backend_hash = "bh-1";
  rm_unreg_req.endpoint = "https://rm-a:9443";
  rc = lc_engine_client_tcrm_unregister(client, &rm_unreg_req, &rm_unreg_res,
                                        &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&rm_list_res, 0, sizeof(rm_list_res));
  rc = lc_engine_client_tcrm_list(client, &rm_list_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(rm_list_res.backend_count, 1U);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);

  lc_engine_namespace_config_response_cleanup(&ns_res);
  lc_engine_index_flush_response_cleanup(&flush_res);
  lc_engine_txn_replay_response_cleanup(&replay_res);
  lc_engine_txn_decision_response_cleanup(&decision_res);
  lc_engine_tc_lease_acquire_response_cleanup(&tc_acquire_res);
  lc_engine_tc_lease_renew_response_cleanup(&tc_renew_res);
  lc_engine_tc_lease_release_response_cleanup(&tc_release_res);
  lc_engine_tc_leader_response_cleanup(&tc_leader_res);
  lc_engine_tc_cluster_response_cleanup(&cluster_res);
  lc_engine_tcrm_register_response_cleanup(&rm_res);
  lc_engine_tcrm_unregister_response_cleanup(&rm_unreg_res);
  lc_engine_tcrm_list_response_cleanup(&rm_list_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_queue_transport_paths_use_mtls(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-stats",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"waiting_consumers\":1,\"pending_candidates\":2,"
       "\"total_consumers\":3,\"has_active_watcher\":true,"
       "\"available\":true,\"head_message_id\":\"msg-1\","
       "\"head_enqueued_at_unix\":100,\"head_not_visible_until_unix\":101,"
       "\"head_age_seconds\":2}",
       "liblockdc test client"},
      {"POST", "/v1/queue/ack", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\"", "\"message_id\":\"msg-1\"",
                             "\"lease_id\":\"lease-1\"", "\"txn_id\":\"txn-1\"",
                             "\"fencing_token\":9", "\"meta_etag\":\"meta-1\"",
                             "\"state_etag\":\"state-1\"",
                             "\"state_lease_id\":\"state-lease-1\"",
                             "\"state_fencing_token\":10"},
       10U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-ack",
                             "Content-Type: application/json"},
       2U, "{\"acked\":true,\"correlation_id\":\"corr-queue-ack\"}",
       "liblockdc test client"},
      {"POST", "/v1/queue/nack", queue_headers, 1U,
       (const char *const[]){
           "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
           "\"message_id\":\"msg-1\"", "\"lease_id\":\"lease-1\"",
           "\"meta_etag\":\"meta-1\"", "\"delay_seconds\":4",
           "\"intent\":\"defer\"", "\"last_error\":{\"code\":\"oops\"}",
           "\"state_etag\":\"state-1\"", "\"state_lease_id\":\"state-lease-1\"",
           "\"state_fencing_token\":10"},
       11U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-nack",
                             "Content-Type: application/json"},
       2U,
       "{\"requeued\":true,\"meta_etag\":\"meta-2\","
       "\"correlation_id\":\"corr-queue-nack\"}",
       "liblockdc test client"},
      {"POST", "/v1/queue/extend", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\"", "\"message_id\":\"msg-1\"",
                             "\"lease_id\":\"lease-1\"",
                             "\"meta_etag\":\"meta-2\"", "\"txn_id\":\"txn-1\"",
                             "\"fencing_token\":9", "\"extend_by_seconds\":30",
                             "\"state_lease_id\":\"state-lease-1\"",
                             "\"state_fencing_token\":10"},
       10U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-extend",
                             "Content-Type: application/json"},
       2U,
       "{\"lease_expires_at_unix\":500,\"visibility_timeout_seconds\":30,"
       "\"meta_etag\":\"meta-3\",\"state_lease_expires_at_unix\":600,"
       "\"correlation_id\":\"corr-queue-extend\"}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  lc_engine_queue_ack_request ack_req;
  lc_engine_queue_ack_response ack_res;
  lc_engine_queue_nack_request nack_req;
  lc_engine_queue_nack_response nack_res;
  lc_engine_queue_extend_request extend_req;
  lc_engine_queue_extend_response extend_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(stats_res.available);

  memset(&ack_req, 0, sizeof(ack_req));
  memset(&ack_res, 0, sizeof(ack_res));
  ack_req.queue = "jobs";
  ack_req.message_id = "msg-1";
  ack_req.lease_id = "lease-1";
  ack_req.txn_id = "txn-1";
  ack_req.fencing_token = 9L;
  ack_req.meta_etag = "meta-1";
  ack_req.state_etag = "state-1";
  ack_req.state_lease_id = "state-lease-1";
  ack_req.state_fencing_token = 10L;
  rc = lc_engine_client_queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_true(ack_res.acked);

  memset(&nack_req, 0, sizeof(nack_req));
  memset(&nack_res, 0, sizeof(nack_res));
  nack_req.queue = "jobs";
  nack_req.message_id = "msg-1";
  nack_req.lease_id = "lease-1";
  nack_req.meta_etag = "meta-1";
  nack_req.delay_seconds = 4L;
  nack_req.intent = "defer";
  nack_req.last_error_json = "{\"code\":\"oops\"}";
  nack_req.state_etag = "state-1";
  nack_req.state_lease_id = "state-lease-1";
  nack_req.state_fencing_token = 10L;
  rc = lc_engine_client_queue_nack(client, &nack_req, &nack_res, &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg("queue nack rc=%d code=%d http_status=%ld message=%s detail=%s "
             "server=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(none)");
  }
  assert_true(nack_res.requeued);

  memset(&extend_req, 0, sizeof(extend_req));
  memset(&extend_res, 0, sizeof(extend_res));
  extend_req.queue = "jobs";
  extend_req.message_id = "msg-1";
  extend_req.lease_id = "lease-1";
  extend_req.meta_etag = "meta-2";
  extend_req.txn_id = "txn-1";
  extend_req.fencing_token = 9L;
  extend_req.extend_by_seconds = 30L;
  extend_req.state_lease_id = "state-lease-1";
  extend_req.state_fencing_token = 10L;
  rc = lc_engine_client_queue_extend(client, &extend_req, &extend_res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(extend_res.visibility_timeout_seconds, 30L);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);

  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_queue_ack_response_cleanup(&ack_res);
  lc_engine_queue_nack_response_cleanup(&nack_res);
  lc_engine_queue_extend_response_cleanup(&extend_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_queue_transport_rejects_oversized_error_body(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  char *error_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 503, (const char *const[]){"Content-Type: application/json"},
       1U, NULL, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  error_body = make_repeat_json_body("{\"error\":\"", "\"}", 9000U, 'x');
  assert_non_null(error_body);
  expectations[0].response_body = error_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "byte limit"));

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(error_body);
}

static void test_watch_transport_rejects_oversized_error_body(void **state) {
  static const char *watch_headers[] = {"Content-Type: application/json",
                                        "Accept: text/event-stream"};
  char *error_body;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/watch", watch_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 503, (const char *const[]){"Content-Type: application/json"},
       1U, NULL, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_watch_queue_request watch_req;
  int rc;

  (void)state;
  error_body = make_repeat_json_body("{\"error\":\"", "\"}", 9000U, 'w');
  assert_non_null(error_body);
  expectations[0].response_body = error_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&watch_req, 0, sizeof(watch_req));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  watch_req.queue = "jobs";
  rc = lc_engine_client_watch_queue(client, &watch_req, watch_event_sink, NULL,
                                    &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_NO_MEMORY);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "watch_queue error body"));

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(error_body);
}

static void
test_queue_transport_rejects_overflowing_numeric_fields(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-stats-overflow",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"waiting_consumers\":2147483648}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_queue_transport_preserves_typed_json_parse_errors(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char malformed_body[] = "{";
  https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-queue-stats-bad-json",
                             "Content-Type: application/json"},
       2U, malformed_body, "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_error error;
  lc_engine_queue_stats_request stats_req;
  lc_engine_queue_stats_response stats_res;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&error, 0, sizeof(error));
  memset(&stats_req, 0, sizeof(stats_req));
  memset(&stats_res, 0, sizeof(stats_res));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  stats_req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_int_equal(error.code, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_queue_stats_response_cleanup(&stats_res);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_subscribe_respects_client_meta_limit(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *meta_body;
  char *response_body;
  size_t response_body_len;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  meta_body = make_repeat_json_body(
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"",
      "\"}", 128U, 'm');
  assert_non_null(meta_body);
  response_body_len = strlen(meta_body) + 256U;
  response_body = (char *)malloc(response_body_len);
  assert_non_null(response_body);
  snprintf(response_body, response_body_len,
           "--queue-boundary\r\n"
           "Content-Disposition: form-data; name=\"meta\"\r\n"
           "Content-Type: application/json\r\n"
           "Content-Length: %zu \r\n"
           "\r\n"
           "%s\r\n"
           "--queue-boundary--\r\n",
           strlen(meta_body), meta_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  config.http_json_response_limit_bytes = 64U;
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_NO_MEMORY);
  assert_non_null(error.message);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(meta_body);
  free(response_body);
}

static void test_subscribe_rejects_default_meta_overflow(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  char *meta_body;
  char *response_body;
  size_t response_body_len;
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), NULL,
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  meta_body = make_repeat_json_body(
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"",
      "\"}", 6000U, 'm');
  assert_non_null(meta_body);
  response_body_len = strlen(meta_body) + 256U;
  response_body = (char *)malloc(response_body_len);
  assert_non_null(response_body);
  snprintf(response_body, response_body_len,
           "--queue-boundary\r\n"
           "Content-Disposition: form-data; name=\"meta\"\r\n"
           "Content-Type: application/json\r\n"
           "Content-Length: %zu \r\n"
           "\r\n"
           "%s\r\n"
           "--queue-boundary--\r\n",
           strlen(meta_body), meta_body);
  expectations[0].response_body = response_body;

  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_NO_MEMORY);
  assert_int_equal(error.code, LC_ENGINE_ERROR_NO_MEMORY);
  assert_int_equal(capture.begin_calls, 0);
  assert_int_equal(capture.end_calls, 0);

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
  free(meta_body);
  free(response_body);
}

static void
test_state_transport_accepts_numeric_headers_with_trailing_ows(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-get-ows", "Content-Type: application/json",
      "ETag: etag-2", "X-Key-Version: 5 \t", "X-Fencing-Token: 11 "};
  https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_get_request req;
  lc_engine_get_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.key = "resource/1";
  req.public_read = 1;
  rc = lc_engine_client_get(client, &req, &res, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(res.version, 5L);
  assert_int_equal(res.fencing_token, 11L);

  lc_engine_get_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_state_transport_rejects_invalid_numeric_headers_as_protocol(void **state) {
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-get-bad-header", "Content-Type: application/json",
      "X-Key-Version: not-a-number"};
  https_expectation expectations[] = {
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_get_request req;
  lc_engine_get_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.key = "resource/1";
  req.public_read = 1;
  rc = lc_engine_client_get(client, &req, &res, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_PROTOCOL);
  assert_non_null(error.message);

  lc_engine_get_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_subscribe_accepts_content_length_with_trailing_ows(void **state) {
  static const char response_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192 \t\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\"},"
      "\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11 \t\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-subscribe",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  https_expectation expectations[] = {
      {"POST", "/v1/queue/subscribe",
       (const char *const[]){"Content-Type: application/json"}, 1U,
       (const char *const[]){"\"namespace\":\"transport-ns\"",
                             "\"queue\":\"jobs\""},
       2U, 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]), response_body,
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_dequeue_request req;
  lc_engine_queue_stream_handler handler;
  subscribe_capture capture;
  lc_engine_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_client_config(&config, server.port, material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  req.owner = "worker-1";
  handler.begin = capture_delivery_begin;
  handler.chunk = capture_delivery_chunk;
  handler.end = capture_delivery_end;
  rc = lc_engine_client_subscribe(client, &req, &handler, &capture, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(capture.begin_calls, 1);
  assert_int_equal(capture.end_calls, 1);
  assert_string_equal(capture.message_id, "msg-1");
  assert_string_equal(capture.payload, "{\"ok\":true}");

  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_emits_pslog_messages(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body,
       sizeof(acquire_body) / sizeof(acquire_body[0]), 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-acquire",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req req;
  lc_lease *lease;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&error, 0, sizeof(error));
  client = NULL;
  lease = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  req.key = "resource/1";
  req.owner = "owner-a";
  req.ttl_seconds = 30L;
  rc = client->acquire(client, &req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lease->close(lease);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.init\""));
  assert_non_null(strstr(logs, "\"message\":\"client.http.attempt\""));
  assert_non_null(strstr(logs, "\"message\":\"client.http.success\""));
  assert_non_null(strstr(logs, "\"message\":\"client.acquire.success\""));
  assert_non_null(strstr(logs, "\"sys\":\"client.lockd\""));
  assert_non_null(strstr(logs, "\"key\":\"resource/1\""));
  assert_non_null(strstr(logs, "\"lease_id\":\"lease-1\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-acquire\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_client_can_disable_sdk_sys_field(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body,
       sizeof(acquire_body) / sizeof(acquire_body[0]), 0, 200,
       (const char *const[]){"X-Correlation-Id: corr-acquire",
                             "Content-Type: application/json"},
       2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req req;
  lc_lease *lease;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&error, 0, sizeof(error));
  client = NULL;
  lease = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  config.disable_logger_sys_field = 1;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  req.key = "resource/1";
  req.owner = "owner-a";
  req.ttl_seconds = 30L;
  rc = client->acquire(client, &req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lease->close(lease);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.init\""));
  assert_non_null(strstr(logs, "\"message\":\"client.acquire.success\""));
  assert_null(strstr(logs, "\"sys\":\"client.lockd\""));
  assert_null(strstr(logs, "\"sys\":\"client.sdk\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_bound_lease_methods_emit_logs(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *keepalive_request_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11"};
  static const char *keepalive_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"lease_id\":\"lease-1\"", "\"ttl_seconds\":45",
      "\"txn_id\":\"txn-acquire\""};
  static const char *keepalive_response_headers[] = {
      "X-Correlation-Id: corr-keepalive", "Content-Type: application/json"};
  static const char *release_request_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11"};
  static const char *release_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"lease_id\":\"lease-1\"", "\"txn_id\":\"txn-acquire\"",
      "\"rollback\":true"};
  static const char *release_response_headers[] = {
      "X-Correlation-Id: corr-release", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST", "/v1/keepalive", keepalive_request_headers, 2U, keepalive_body,
       5U, 0, 200, keepalive_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"lease_id\":\"lease-1\",\"txn_id\":\"txn-acquire\","
       "\"expires_at_unix\":2000,\"version\":5,\"state_etag\":\"etag-2\"}",
       "liblockdc test client"},
      {"POST", "/v1/release", release_request_headers, 2U, release_body, 5U, 0,
       200, release_response_headers, 2U, "{\"released\":true}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_keepalive_req keepalive_req;
  lc_release_req release_req;
  lc_error error;
  lc_lease *lease;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  client = NULL;
  lease = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&keepalive_req, 0, sizeof(keepalive_req));
  memset(&release_req, 0, sizeof(release_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  keepalive_req.ttl_seconds = 45L;
  rc = lc_lease_keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);

  release_req.rollback = 1;
  rc = lc_lease_release(lease, &release_req, &error);
  if (rc != LC_OK) {
    fail_msg("lease release rc=%d code=%d http_status=%ld message=%s "
             "detail=%s server=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(none)");
  }
  lease = NULL;

  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.keepalive.start\""));
  assert_non_null(strstr(logs, "\"message\":\"client.keepalive.success\""));
  assert_non_null(strstr(logs, "\"message\":\"client.release.start\""));
  assert_non_null(strstr(logs, "\"message\":\"client.release.success\""));
  assert_non_null(strstr(logs, "\"lease_id\":\"lease-1\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-keepalive\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-release\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_bound_lease_methods_cover_state_and_attachments(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *describe_response_headers[] = {
      "X-Correlation-Id: corr-describe", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-Version: 4"};
  static const char *mutate_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-Version: 5"};
  static const char *metadata_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-Version: 6"};
  static const char *remove_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-State-ETag: etag-4", "X-If-Version: 7"};
  static const char *attach_headers[] = {
      "Content-Type: text/plain", "X-Lease-ID: lease-1",
      "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *metadata_body[] = {"\"namespace\":\"transport-ns\"",
                                        "\"query_hidden\":false"};
  static const char *mutate_body[] = {"\"namespace\":\"transport-ns\"",
                                      "\"mutations\":[\"set($.value,3)\","
                                      "\"set($.ready,true)\"]"};
  static const char *list_headers[] = {
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *attachment_download_headers[] = {
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *attachment_response_headers[] = {
      "X-Correlation-Id: corr-attachment-get",
      "Content-Type: text/plain",
      "X-Attachment-ID: att-1",
      "X-Attachment-Name: blob.txt",
      "X-Attachment-SHA256: sha-1",
      "X-Attachment-Size: 11",
      "X-Attachment-Created-At: 100",
      "X-Attachment-Updated-At: 101"};
  static const char *attachment_delete_headers[] = {
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *attachment_delete_all_headers[] = {
      "X-Lease-ID: lease-1", "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-update", "Content-Type: application/json"};
  static const char *mutate_response_headers[] = {
      "X-Correlation-Id: corr-mutate", "Content-Type: application/json"};
  static const char *metadata_response_headers[] = {
      "X-Correlation-Id: corr-metadata", "Content-Type: application/json"};
  static const char *attach_response_headers[] = {
      "X-Correlation-Id: corr-attach", "Content-Type: application/json"};
  static const char *list_response_headers[] = {
      "X-Correlation-Id: corr-attachment-list",
      "Content-Type: application/json"};
  static const char *delete_response_headers[] = {
      "X-Correlation-Id: corr-attachment-delete",
      "Content-Type: application/json"};
  static const char *delete_all_response_headers[] = {
      "X-Correlation-Id: corr-attachment-delete-all",
      "Content-Type: application/json"};
  static const char *remove_response_headers[] = {
      "X-Correlation-Id: corr-remove", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"GET", "/v1/describe?key=resource%2F1&namespace=transport-ns", NULL, 0U,
       NULL, 0U, 1, 200, describe_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"expires_at_unix\":1000,\"version\":4,\"state_etag\":\"etag-1\","
       "\"updated_at_unix\":2000,\"metadata\":{\"query_hidden\":true}}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"value\":1}",
       "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns",
       update_headers, sizeof(update_headers) / sizeof(update_headers[0]), NULL,
       0U, 0, 200, update_response_headers, 2U,
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\",\"bytes\":11}",
       "liblockdc test client"},
      {"POST", "/v1/mutate?key=resource%2F1&namespace=transport-ns",
       mutate_headers, sizeof(mutate_headers) / sizeof(mutate_headers[0]),
       mutate_body, sizeof(mutate_body) / sizeof(mutate_body[0]), 0, 200,
       mutate_response_headers, 2U,
       "{\"new_version\":6,\"new_state_etag\":\"etag-3\",\"bytes\":21}",
       "liblockdc test client"},
      {"POST", "/v1/metadata?key=resource%2F1&namespace=transport-ns",
       metadata_headers, 3U, metadata_body, 2U, 0, 200,
       metadata_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"version\":7,\"metadata\":{\"query_hidden\":false}}",
       "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, 4U, NULL, 0U, 0, 200, attach_response_headers, 2U,
       "{\"attachment\":{\"id\":\"att-1\",\"name\":\"blob.txt\","
       "\"size\":11,\"plaintext_sha256\":\"sha-1\","
       "\"content_type\":\"text/plain\",\"created_at_unix\":100,"
       "\"updated_at_unix\":101},\"noop\":false,\"version\":8}",
       "liblockdc test client"},
      {"GET", "/v1/attachments?key=resource%2F1&namespace=transport-ns",
       list_headers, 3U, NULL, 0U, 1, 200, list_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"attachments\":[{\"id\":\"att-1\",\"name\":\"blob.txt\","
       "\"size\":11,\"plaintext_sha256\":\"sha-1\","
       "\"content_type\":\"text/plain\",\"created_at_unix\":100,"
       "\"updated_at_unix\":101}]}",
       "liblockdc test client"},
      {"GET",
       "/v1/attachment?key=resource%2F1&namespace=transport-ns&name=blob.txt",
       attachment_download_headers, 3U, NULL, 0U, 0, 200,
       attachment_response_headers, 8U, "hello world", "liblockdc test client"},
      {"DELETE",
       "/v1/attachment?key=resource%2F1&namespace=transport-ns&name=blob.txt",
       attachment_delete_headers, 3U, NULL, 0U, 1, 200, delete_response_headers,
       2U, "{\"deleted\":true,\"version\":9}", "liblockdc test client"},
      {"DELETE", "/v1/attachments?key=resource%2F1&namespace=transport-ns",
       attachment_delete_all_headers, 3U, NULL, 0U, 1, 200,
       delete_all_response_headers, 2U, "{\"deleted\":1,\"version\":10}",
       "liblockdc test client"},
      {"POST", "/v1/remove?key=resource%2F1&namespace=transport-ns",
       remove_headers, 5U, NULL, 0U, 1, 200, remove_response_headers, 2U,
       "{\"removed\":true,\"new_version\":11}", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_error error;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_json *json;
  lc_update_opts update_opts;
  const char *mutations[2];
  lc_mutate_req mutate_req;
  lc_metadata_req metadata_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_source *src;
  lc_attachment_list attachment_list;
  lc_attachment_get_req attachment_get_req;
  lc_attachment_get_res attachment_get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  lc_attachment_selector selector;
  int deleted;
  int deleted_count;
  lc_remove_req remove_req;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  json = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  deleted = 0;
  deleted_count = 0;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&error, 0, sizeof(error));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&update_opts, 0, sizeof(update_opts));
  memset(&mutate_req, 0, sizeof(mutate_req));
  memset(&metadata_req, 0, sizeof(metadata_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&attachment_get_req, 0, sizeof(attachment_get_req));
  memset(&attachment_get_res, 0, sizeof(attachment_get_res));
  memset(&selector, 0, sizeof(selector));
  memset(&remove_req, 0, sizeof(remove_req));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  rc = lc_lease_describe(lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(lease->state_etag, "etag-1");
  assert_int_equal(lease->version, 4L);

  get_opts.public_read = 1;
  rc = lc_lease_load(lease, &test_value_map, &value_doc, NULL, &get_opts,
                     &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(value_doc.value, 1);
  assert_string_equal(get_res.etag, "etag-1");
  lonejson_cleanup(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);

  rc = lc_json_from_string("{\"value\":2}", &json, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_update(lease, json, &update_opts, &error);
  assert_int_equal(rc, LC_OK);
  lc_json_close(json);
  json = NULL;
  assert_string_equal(lease->state_etag, "etag-2");
  assert_int_equal(lease->version, 5L);

  mutations[0] = "set($.value,3)";
  mutations[1] = "set($.ready,true)";
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 2U;
  rc = lc_lease_mutate(lease, &mutate_req, &error);
  if (rc != LC_OK) {
    fail_msg("lease mutate rc=%d code=%d http_status=%ld message=%s detail=%s "
             "server_failure=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(none)");
  }
  assert_int_equal(rc, LC_OK);
  assert_string_equal(lease->state_etag, "etag-3");
  assert_int_equal(lease->version, 6L);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 0;
  metadata_req.if_version = 6L;
  metadata_req.has_if_version = 1;
  rc = lc_lease_metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 7L);
  assert_true(lease->has_query_hidden);
  assert_false(lease->query_hidden);

  rc = lc_source_from_memory("hello world", 11U, &src, &error);
  assert_int_equal(rc, LC_OK);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach_req, src, &attach_res, &error);
  if (rc != LC_OK) {
    fail_msg("lease attach retry rc=%d code=%d http_status=%ld message=%s "
             "server=%s handled=%zu",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(null)",
             server.handled_count);
  }
  assert_int_equal(rc, LC_OK);
  assert_string_equal(attach_res.attachment.id, "att-1");
  assert_string_equal(attach_res.attachment.name, "blob.txt");
  assert_false(attach_res.noop);
  lc_attach_res_cleanup(&attach_res);
  lc_source_close(src);
  src = NULL;

  rc = lc_lease_list_attachments(lease, &attachment_list, &error);
  if (rc != LC_OK) {
    fail_msg("list attachments rc=%d code=%d http_status=%ld message=%s "
             "detail=%s server_failure=%s",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(none)");
  }
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].id, "att-1");
  lc_attachment_list_cleanup(&attachment_list);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  attachment_get_req.selector.name = "blob.txt";
  rc = lc_lease_get_attachment(lease, &attachment_get_req, sink,
                               &attachment_get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, 11U);
  assert_memory_equal(bytes, "hello world", 11U);
  assert_string_equal(attachment_get_res.attachment.id, "att-1");
  lc_attachment_get_res_cleanup(&attachment_get_res);
  lc_sink_close(sink);
  sink = NULL;

  selector.name = "blob.txt";
  rc = lc_lease_delete_attachment(lease, &selector, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(deleted);

  rc = lc_lease_delete_all_attachments(lease, &deleted_count, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 1);

  remove_req.if_state_etag = "etag-4";
  remove_req.if_version = 7L;
  remove_req.has_if_version = 1;
  rc = lc_lease_remove(lease, &remove_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(lease->state_etag);
  assert_int_equal(lease->version, 11L);

  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_attach_rejects_malformed_json_response(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *attach_headers[] = {
      "Content-Type: text/plain", "X-Lease-ID: lease-1",
      "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-bad-json",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]),
       NULL, 0U, 0, 200, attach_response_headers, 2U, "{",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_source *src;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  rc = lc_source_from_memory("hello world", 11U, &src, &error);
  assert_int_equal(rc, LC_OK);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach_req, src, &attach_res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_non_null(error.message);

  lc_attach_res_cleanup(&attach_res);
  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_attach_retries_node_passive_and_cleans_parser_state(
    void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *attach_headers[] = {
      "Content-Type: text/plain", "X-Lease-ID: lease-1",
      "X-Txn-ID: txn-acquire", "X-Fencing-Token: 11"};
  static const char *first_attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-passive-1",
      "Content-Type: application/json"};
  static const char *second_attach_response_headers[] = {
      "X-Correlation-Id: corr-attach-passive-2",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]),
       NULL, 0U, 0, 503, first_attach_response_headers, 2U,
       "{\"error\":\"node_passive\"}", "liblockdc test client"},
      {"POST",
       "/v1/attachments?key=resource%2F1&namespace=transport-ns&name=blob.txt&"
       "content_type=text%2Fplain",
       attach_headers, sizeof(attach_headers) / sizeof(attach_headers[0]),
       NULL, 0U, 0, 200, second_attach_response_headers, 2U,
       "{\"attachment\":{\"id\":\"att-1\",\"name\":\"blob.txt\","
       "\"size\":11,\"plaintext_sha256\":\"sha-1\","
       "\"content_type\":\"text/plain\",\"created_at_unix\":1000,"
       "\"updated_at_unix\":1001},\"noop\":false,\"version\":5}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_source *src;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  src = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  static char endpoint_a[128];
  static char endpoint_b[128];
  static const char *endpoints[2];
  snprintf(endpoint_a, sizeof(endpoint_a), "https://127.0.0.1:%u",
           (unsigned)server.port);
  snprintf(endpoint_b, sizeof(endpoint_b), "https://127.0.0.1:%u",
           (unsigned)server.port);
  endpoints[0] = endpoint_a;
  endpoints[1] = endpoint_b;
  config.endpoints = endpoints;
  config.endpoint_count = 2U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  rc = lc_source_from_memory("hello world", 11U, &src, &error);
  assert_int_equal(rc, LC_OK);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  rc = lc_lease_attach(lease, &attach_req, src, &attach_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(attach_res.attachment.id, "att-1");
  assert_string_equal(attach_res.attachment.name, "blob.txt");
  assert_false(attach_res.noop);
  assert_int_equal(attach_res.version, 5L);

  lc_attach_res_cleanup(&attach_res);
  lc_source_close(src);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_queue_transport_retries_node_passive_and_cleans_parser_state(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *const queue_stats_body[] = {"\"namespace\":\"transport-ns\"",
                                                 "\"queue\":\"jobs\""};
  static const char *first_response_headers[] = {
      "X-Correlation-Id: corr-queue-stats-passive-1",
      "Content-Type: application/json"};
  static const char *second_response_headers[] = {
      "X-Correlation-Id: corr-queue-stats-passive-2",
      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       queue_stats_body, sizeof(queue_stats_body) / sizeof(queue_stats_body[0]),
       0, 503, first_response_headers,
       sizeof(first_response_headers) / sizeof(first_response_headers[0]),
       "{\"error\":\"node_passive\"}", "liblockdc test client"},
      {"POST", "/v1/queue/stats", queue_headers, 1U,
       queue_stats_body, sizeof(queue_stats_body) / sizeof(queue_stats_body[0]),
       0, 200, second_response_headers,
       sizeof(second_response_headers) / sizeof(second_response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"waiting_consumers\":1,\"pending_candidates\":2,"
       "\"total_consumers\":3,\"has_active_watcher\":false,"
       "\"available\":true,\"head_message_id\":\"msg-1\","
       "\"head_enqueued_at_unix\":100,\"head_not_visible_until_unix\":101,"
       "\"head_age_seconds\":2}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_queue_stats_request req;
  lc_engine_queue_stats_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_client_config_two_endpoints(&config, server.port,
                                   material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.queue = "jobs";
  rc = lc_engine_client_queue_stats(client, &req, &res, &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg("queue_stats retry rc=%d code=%d http_status=%ld message=%s "
             "server=%s detail=%s handled=%zu",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             error.server_error_code != NULL ? error.server_error_code
                                             : "(null)",
             error.detail != NULL ? error.detail : "(null)",
             server.handled_count);
  }
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_int_equal(res.waiting_consumers, 1);
  assert_int_equal(res.pending_candidates, 2);
  assert_int_equal(res.total_consumers, 3);
  assert_true(res.available);
  assert_false(res.has_active_watcher);
  assert_string_equal(res.head_message_id, "msg-1");

  lc_engine_queue_stats_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_enqueue_from_retries_node_passive_and_cleans_parser_state(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *enqueue_response_headers[] = {
      "X-Correlation-Id: corr-enqueue-passive-2",
      "Content-Type: application/json"};
  static const char *const request_body_substrings[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "name=\"meta\"", "name=\"payload\""};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", queue_headers, 1U,
       request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]),
       0, 503, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"error\":\"node_passive\"}", "liblockdc test client"},
      {"POST", "/v1/queue/enqueue", queue_headers, 1U,
       request_body_substrings,
       sizeof(request_body_substrings) / sizeof(request_body_substrings[0]),
       0, 200, enqueue_response_headers,
       sizeof(enqueue_response_headers) / sizeof(enqueue_response_headers[0]),
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-enqueue-passive-2\",\"attempts\":0,"
       "\"max_attempts\":5,\"failure_attempts\":0,"
       "\"not_visible_until_unix\":123,\"visibility_timeout_seconds\":30,"
       "\"payload_bytes\":0}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_enqueue_request req;
  lc_engine_enqueue_response res;
  lc_engine_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  init_client_config_two_endpoints(&config, server.port,
                                   material.client_bundle_path);
  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  req.namespace_name = "transport-ns";
  req.queue = "jobs";
  req.payload_content_type = "application/json";
  rc = lc_engine_client_enqueue_from(client, &req, NULL, NULL, &res, &error);
  if (rc != LC_ENGINE_OK) {
    fail_msg("enqueue_from retry rc=%d code=%d http_status=%ld message=%s "
             "server=%s handled=%zu",
             rc, error.code, error.http_status,
             error.message != NULL ? error.message : "(null)",
             server.failure_message[0] != '\0' ? server.failure_message
                                               : "(null)",
             server.handled_count);
  }
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(res.namespace_name, "transport-ns");
  assert_string_equal(res.queue, "jobs");
  assert_string_equal(res.message_id, "msg-enqueue-passive-2");
  assert_string_equal(res.correlation_id, "corr-enqueue-passive-2");
  assert_int_equal(res.payload_bytes, 0L);

  lc_engine_enqueue_response_cleanup(&res);
  lc_engine_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_engine_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_mutate_local_covers_no_content_path(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/2\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-b\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire-local", "Content-Type: application/json"};
  static const char *get_headers[] = {"X-Correlation-Id: corr-local-get",
                                      "Content-Type: application/json"};
  static const char *update_headers[] = {"Content-Type: application/json",
                                         "X-Fencing-Token: 22",
                                         "X-Txn-ID: txn-local"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-local-update", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/2\","
       "\"owner\":\"owner-b\",\"lease_id\":\"lease-2\","
       "\"txn_id\":\"txn-local\",\"expires_at_unix\":1000,"
       "\"version\":0,\"state_etag\":\"\",\"fencing_token\":22}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F2&namespace=transport-ns", NULL, 0U, NULL,
       0U, 1, 204, get_headers, 2U, NULL, "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F2&namespace=transport-ns",
       update_headers, 2U, NULL, 0U, 0, 200, update_response_headers, 2U,
       "{\"new_version\":1,\"new_state_etag\":\"etag-local\",\"bytes\":17}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_mutate_local_req mutate_local_req;
  const char *mutations[1];
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&mutate_local_req, 0, sizeof(mutate_local_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/2";
  acquire_req.owner = "owner-b";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  mutations[0] = "/extra=\"value\"";
  mutate_local_req.mutations = mutations;
  mutate_local_req.mutation_count = 1U;
  rc = lc_lease_mutate_local(lease, &mutate_local_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(lease->state_etag, "etag-local");
  assert_int_equal(lease->version, 1L);

  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_management_methods_emit_logs(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *ns_response_headers[] = {"X-Correlation-Id: corr-ns-get",
                                              "Content-Type: application/json"};
  static const char *tc_body[] = {"\"candidate_id\":\"node-a\"",
                                  "\"candidate_endpoint\":"
                                  "\"https://node-a:9443\"",
                                  "\"term\":3", "\"ttl_ms\":2000"};
  static const char *tc_response_headers[] = {
      "X-Correlation-Id: corr-tc-acquire", "Content-Type: application/json"};
  static const char *rm_response_headers[] = {"X-Correlation-Id: corr-rm-list",
                                              "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"GET", "/v1/namespace?namespace=team-a", NULL, 0U, NULL, 0U, 1, 200,
       ns_response_headers, 2U,
       "{\"namespace\":\"team-a\",\"query\":{\"preferred_engine\":\"index\","
       "\"fallback_engine\":\"scan\"}}",
       "liblockdc test client"},
      {"POST", "/v1/tc/lease/acquire", json_header, 1U, tc_body, 4U, 0, 200,
       tc_response_headers, 2U,
       "{\"granted\":true,\"leader_id\":\"node-a\","
       "\"leader_endpoint\":\"https://node-a:9443\",\"term\":3,"
       "\"expires_at\":9000}",
       "liblockdc test client"},
      {"GET", "/v1/tc/rm/list", NULL, 0U, NULL, 0U, 1, 200, rm_response_headers,
       2U,
       "{\"backends\":[{\"backend_hash\":\"bh-1\","
       "\"endpoints\":[\"https://rm-b:9443\"],\"updated_at_unix\":302}],"
       "\"updated_at_unix\":303}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_namespace_config_req ns_req;
  lc_namespace_config_res ns_res;
  lc_tc_lease_acquire_req tc_req;
  lc_tc_lease_acquire_res tc_res;
  lc_tc_rm_list_res rm_res;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  client = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  memset(&config, 0, sizeof(config));
  memset(&ns_req, 0, sizeof(ns_req));
  memset(&ns_res, 0, sizeof(ns_res));
  memset(&tc_req, 0, sizeof(tc_req));
  memset(&tc_res, 0, sizeof(tc_res));
  memset(&rm_res, 0, sizeof(rm_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  ns_req.namespace_name = "team-a";
  rc = lc_get_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_OK);

  tc_req.candidate_id = "node-a";
  tc_req.candidate_endpoint = "https://node-a:9443";
  tc_req.term = 3UL;
  tc_req.ttl_ms = 2000L;
  rc = lc_tc_lease_acquire(client, &tc_req, &tc_res, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_tc_rm_list(client, &rm_res, &error);
  assert_int_equal(rc, LC_OK);

  lc_namespace_config_res_cleanup(&ns_res);
  lc_tc_lease_acquire_res_cleanup(&tc_res);
  lc_tc_rm_list_res_cleanup(&rm_res);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.namespace.get.start\""));
  assert_non_null(strstr(logs, "\"message\":\"client.namespace.get.success\""));
  assert_non_null(
      strstr(logs, "\"message\":\"client.tc.lease.acquire.start\""));
  assert_non_null(
      strstr(logs, "\"message\":\"client.tc.lease.acquire.success\""));
  assert_non_null(strstr(logs, "\"message\":\"client.rm.list.start\""));
  assert_non_null(strstr(logs, "\"message\":\"client.rm.list.success\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-rm-list\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_queue_nack_maps_enum_intents(void **state) {
  static const char *queue_headers[] = {"Content-Type: application/json"};
  static const char *const nack_failure_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "\"message_id\":\"msg-failure\"", "\"lease_id\":\"lease-failure\"",
      "\"meta_etag\":\"meta-failure\"", "\"delay_seconds\":2",
      "\"intent\":\"failure\""};
  static const char *const nack_failure_headers[] = {
      "X-Correlation-Id: corr-public-nack-1", "Content-Type: application/json"};
  static const char *const nack_defer_body[] = {
      "\"namespace\":\"transport-ns\"",
      "\"queue\":\"jobs\"",
      "\"message_id\":\"msg-defer\"",
      "\"lease_id\":\"lease-defer\"",
      "\"meta_etag\":\"meta-defer\"",
      "\"delay_seconds\":3",
      "\"intent\":\"defer\""};
  static const char *const nack_defer_headers[] = {
      "X-Correlation-Id: corr-public-nack-2", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/nack", queue_headers, 1U, nack_failure_body, 7U, 0,
       200, nack_failure_headers, 2U,
       "{\"requeued\":true,\"meta_etag\":\"meta-after-failure\","
       "\"correlation_id\":\"corr-public-nack-1\"}",
       "liblockdc test client"},
      {"POST", "/v1/queue/nack", queue_headers, 1U, nack_defer_body, 7U, 0, 200,
       nack_defer_headers, 2U,
       "{\"requeued\":true,\"meta_etag\":\"meta-after-defer\","
       "\"correlation_id\":\"corr-public-nack-2\"}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_nack_op req;
  lc_nack_res out;
  lc_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  client = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  lc_nack_op_init(&req);
  req.message.queue = "jobs";
  req.message.message_id = "msg-failure";
  req.message.lease_id = "lease-failure";
  req.message.meta_etag = "meta-failure";
  req.delay_seconds = 2L;
  rc = lc_queue_nack(client, &req, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(out.requeued);
  lc_nack_res_cleanup(&out);

  lc_nack_op_init(&req);
  req.message.queue = "jobs";
  req.message.message_id = "msg-defer";
  req.message.lease_id = "lease-defer";
  req.message.meta_etag = "meta-defer";
  req.delay_seconds = 3L;
  req.intent = LC_NACK_INTENT_DEFER;
  rc = lc_queue_nack(client, &req, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(out.requeued);
  lc_nack_res_cleanup(&out);

  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_enqueue_emits_logs(void **state) {
  static const char payload[] = "{\"ok\":true}";
  static const char *enqueue_response_headers[] = {
      "X-Correlation-Id: corr-enqueue", "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/enqueue", NULL, 0U, NULL, 0U, 0, 200,
       enqueue_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
       "\"message_id\":\"msg-enqueue\",\"attempts\":0,\"max_attempts\":5,"
       "\"failure_attempts\":0,\"not_visible_until_unix\":123,"
       "\"visibility_timeout_seconds\":30,\"payload_bytes\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_enqueue_req req;
  lc_enqueue_res out;
  lc_source *src;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  client = NULL;
  src = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);
  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory(payload, sizeof(payload) - 1U, &src, &error);
  assert_int_equal(rc, LC_OK);

  req.queue = "jobs";
  req.content_type = "application/json";
  req.max_attempts = 5L;
  req.visibility_timeout_seconds = 30L;
  rc = lc_enqueue(client, &req, src, &out, &error);
  assert_int_equal(rc, LC_OK);

  lc_enqueue_res_cleanup(&out);
  lc_source_close(src);
  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.queue.enqueue.begin\""));
  assert_non_null(strstr(logs, "\"message\":\"client.queue.enqueue.success\""));
  assert_non_null(strstr(logs, "\"message_id\":\"msg-enqueue\""));
  assert_non_null(strstr(logs, "\"cid\":\"corr-enqueue\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_queue_nack_rejects_invalid_intent(void **state) {
  https_tls_material material;
  lc_client_config config;
  lc_client *client;
  lc_nack_op req;
  lc_nack_res out;
  lc_error error;
  int rc;

  (void)state;
  assert_true(https_tls_material_init(&material, 1));
  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&out, 0, sizeof(out));
  memset(&error, 0, sizeof(error));
  client = NULL;

  init_public_client_config(&config, 1U, material.client_bundle_path, NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  lc_nack_op_init(&req);
  req.message.queue = "jobs";
  req.message.message_id = "msg-invalid";
  req.message.lease_id = "lease-invalid";
  req.message.meta_etag = "meta-invalid";
  req.intent = (lc_nack_intent)99;
  rc = lc_queue_nack(client, &req, &out, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_client_close(client);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_dequeue_emits_stream_transport_logs(void **state) {
  static const char dequeue_body[] =
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"meta\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 192\r\n"
      "\r\n"
      "{\"message\":{\"namespace\":\"transport-ns\",\"queue\":\"jobs\","
      "\"message_id\":\"msg-1\",\"payload_content_type\":\"application/json\","
      "\"correlation_id\":\"corr-sub\",\"lease_id\":\"lease-1\","
      "\"meta_etag\":\"meta-1\"},\"next_cursor\":\"cursor-1\"}\r\n"
      "--queue-boundary\r\n"
      "Content-Disposition: form-data; name=\"payload\"\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}\r\n"
      "--queue-boundary--\r\n";
  static const char *dequeue_headers[] = {
      "X-Correlation-Id: corr-dequeue",
      "Content-Type: multipart/mixed; boundary=queue-boundary"};
  static const char *const dequeue_required_headers[] = {
      "Content-Type: application/json"};
  static const char *const dequeue_required_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "\"owner\":\"worker-1\""};
  static const char *const ack_required_body[] = {
      "\"namespace\":\"transport-ns\"", "\"queue\":\"jobs\"",
      "\"message_id\":\"msg-1\"", "\"lease_id\":\"lease-1\"",
      "\"meta_etag\":\"meta-1\""};
  static const char *ack_headers[] = {"X-Correlation-Id: corr-ack",
                                      "Content-Type: application/json"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/queue/dequeue", dequeue_required_headers, 1U,
       dequeue_required_body, 3U, 0, 200, dequeue_headers,
       sizeof(dequeue_headers) / sizeof(dequeue_headers[0]), dequeue_body,
       "liblockdc test client"},
      {"POST", "/v1/queue/ack", dequeue_required_headers, 1U, ack_required_body,
       5U, 0, 200, ack_headers, sizeof(ack_headers) / sizeof(ack_headers[0]),
       "{\"acked\":true,\"correlation_id\":\"corr-ack\"}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_dequeue_req req;
  lc_message *message;
  lc_error error;
  pslog_logger *logger;
  FILE *log_fp;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  log_fp = NULL;
  logs = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  logger = open_test_logger(&log_fp);
  assert_non_null(logger);

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&error, 0, sizeof(error));
  client = NULL;
  message = NULL;
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            logger);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);

  req.queue = "jobs";
  req.owner = "worker-1";
  rc = lc_dequeue(client, &req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);

  rc = lc_message_ack(message, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;

  lc_client_close(client);
  logger->destroy(logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.queue.dequeue.begin\""));
  assert_non_null(strstr(logs, "\"message\":\"client.http.attempt\""));
  assert_non_null(strstr(logs, "\"path\":\"/v1/queue/dequeue\""));
  assert_non_null(strstr(logs, "\"message\":\"client.http.success\""));
  assert_non_null(strstr(logs, "\"message\":\"client.queue.dequeue.success\""));
  assert_non_null(strstr(logs, "\"message\":\"client.queue.ack.success\""));
  assert_non_null(strstr(logs, "\"message_id\":\"msg-1\""));

  free(logs);
  fclose(log_fp);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_stream_captures_headers_and_body(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"namespace\":\"transport-ns\"",
                                     "\"selector\":{\"owner\":\"owner-a\"}",
                                     "\"limit\":2",
                                     "\"cursor\":\"cursor-0\"",
                                     "\"fields\":[\"key\"]",
                                     "\"return\":\"compact\""};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-stream",
      "X-Lockd-Query-Cursor: cursor-1",
      "X-Lockd-Query-Index-Seq: 12",
      "X-Lockd-Query-Metadata: {\"partial\":false}",
      "X-Lockd-Query-Return: compact",
      "Content-Type: application/x-ndjson"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/1\"}\n", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.namespace_name = "transport-ns";
  req.selector_json = "{\"owner\":\"owner-a\"}";
  req.limit = 2L;
  req.cursor = "cursor-0";
  req.fields_json = "[\"key\"]";
  req.return_mode = "compact";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"key\":\"resource/1\"}\n"));
  assert_memory_equal(bytes, "{\"key\":\"resource/1\"}\n", length);
  assert_string_equal(res.cursor, "cursor-1");
  assert_string_equal(res.return_mode, "compact");
  assert_int_equal(res.index_seq, 12UL);
  assert_string_equal(res.correlation_id, "corr-query-stream");

  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_lease_save_uses_mapped_lonejson_upload(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *update_headers[] = {
      "Content-Type: application/json", "X-Fencing-Token: 11",
      "X-Txn-ID: txn-acquire", "X-If-Version: 4"};
  static const char *update_response_headers[] = {
      "X-Correlation-Id: corr-update", "Content-Type: application/json"};
  static const char *update_body[] = {"{\"value\":2}"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"POST", "/v1/update?key=resource%2F1&namespace=transport-ns",
       update_headers, sizeof(update_headers) / sizeof(update_headers[0]),
       update_body, 1U, 0, 200,
       update_response_headers, 2U,
       "{\"new_version\":5,\"new_state_etag\":\"etag-2\","
       "\"bytes\":11}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  value_doc.value = 2;
  rc = lc_lease_save(lease, &test_value_map, &value_doc, NULL, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 5L);
  assert_string_equal(lease->state_etag, "etag-2");

  lonejson_cleanup(&test_value_map, &value_doc);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void
test_public_lease_load_respects_configured_json_response_limit(void **state) {
  static const char *json_header[] = {"Content-Type: application/json"};
  static const char *acquire_body[] = {
      "\"namespace\":\"transport-ns\"", "\"key\":\"resource/1\"",
      "\"ttl_seconds\":30", "\"owner\":\"owner-a\""};
  static const char *acquire_response_headers[] = {
      "X-Correlation-Id: corr-acquire", "Content-Type: application/json"};
  static const char *get_headers[] = {
      "X-Correlation-Id: corr-get", "Content-Type: application/json",
      "ETag: etag-1", "X-Key-Version: 4", "X-Fencing-Token: 11"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/acquire", json_header, 1U, acquire_body, 4U, 0, 200,
       acquire_response_headers, 2U,
       "{\"namespace\":\"transport-ns\",\"key\":\"resource/1\","
       "\"owner\":\"owner-a\",\"lease_id\":\"lease-1\","
       "\"txn_id\":\"txn-acquire\",\"expires_at_unix\":1000,"
       "\"version\":4,\"state_etag\":\"etag-1\",\"fencing_token\":11}",
       "liblockdc test client"},
      {"GET", "/v1/get?key=resource%2F1&namespace=transport-ns&public=1", NULL,
       0U, NULL, 0U, 1, 200, get_headers, 5U, "{\"value\":1}",
       "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_get_opts get_opts;
  lc_get_res get_res;
  test_value_doc value_doc;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  memset(&value_doc, 0, sizeof(value_doc));
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  config.http_json_response_limit_bytes = 0U;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  acquire_req.key = "resource/1";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  ((lc_client_handle *)client)->http_json_response_limit_bytes = 1U;
  get_opts.public_read = 1;
  rc = lc_lease_load(lease, &test_value_map, &value_doc, NULL, &get_opts,
                     &get_res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_string_equal(error.message,
                      "mapped state response exceeds configured byte limit");
  assert_int_equal(value_doc.value, 0);

  lonejson_cleanup(&test_value_map, &value_doc);
  lc_get_res_cleanup(&get_res);
  lc_lease_close(lease);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

static void test_public_query_stream_rejects_invalid_index_seq(void **state) {
  static const char *query_headers[] = {
      "Content-Type: application/json",
      "Accept: application/x-ndjson, application/json"};
  static const char *query_body[] = {"\"selector\":{\"owner\":\"owner-a\"}"};
  static const char *response_headers[] = {
      "X-Correlation-Id: corr-query-stream",
      "X-Lockd-Query-Index-Seq: not-a-number",
      "Content-Type: application/x-ndjson"};
  static const https_expectation expectations[] = {
      {"POST", "/v1/query", query_headers,
       sizeof(query_headers) / sizeof(query_headers[0]), query_body,
       sizeof(query_body) / sizeof(query_body[0]), 0, 200, response_headers,
       sizeof(response_headers) / sizeof(response_headers[0]),
       "{\"key\":\"resource/1\"}\n", "liblockdc test client"}};
  https_tls_material material;
  https_testserver server;
  lc_client_config config;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  lc_sink *sink;
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  assert_true(https_tls_material_init(&material, 1));
  assert_true(
      https_testserver_start(&server, &material, expectations,
                             sizeof(expectations) / sizeof(expectations[0])));

  memset(&config, 0, sizeof(config));
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  init_public_client_config(&config, server.port, material.client_bundle_path,
                            NULL);
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  req.selector_json = "{\"owner\":\"owner-a\"}";
  rc = lc_query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_string_equal(error.message, "failed to parse query index sequence");
  assert_null(res.cursor);
  assert_null(res.return_mode);
  assert_null(res.correlation_id);
  assert_int_equal(res.index_seq, 0UL);

  lc_sink_close(sink);
  lc_client_close(client);
  https_testserver_stop(&server);
  assert_server_ok(&server);
  lc_error_cleanup(&error);
  https_tls_material_cleanup(&material);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_client_open_rejects_bundle_without_ca),
      cmocka_unit_test(test_state_transport_paths_use_mtls),
      cmocka_unit_test(
          test_state_transport_accepts_numeric_headers_with_trailing_ows),
      cmocka_unit_test(
          test_state_transport_rejects_invalid_numeric_headers_as_protocol),
      cmocka_unit_test(test_management_transport_paths_use_mtls),
      cmocka_unit_test(test_queue_transport_paths_use_mtls),
      cmocka_unit_test(test_queue_transport_rejects_oversized_error_body),
      cmocka_unit_test(test_watch_transport_rejects_oversized_error_body),
      cmocka_unit_test(test_queue_transport_rejects_overflowing_numeric_fields),
      cmocka_unit_test(
          test_queue_transport_preserves_typed_json_parse_errors),
      cmocka_unit_test(test_subscribe_accepts_content_length_with_trailing_ows),
      cmocka_unit_test(test_subscribe_respects_client_meta_limit),
      cmocka_unit_test(test_subscribe_rejects_default_meta_overflow),
      cmocka_unit_test(test_public_client_emits_pslog_messages),
      cmocka_unit_test(test_public_client_can_disable_sdk_sys_field),
      cmocka_unit_test(test_public_bound_lease_methods_emit_logs),
      cmocka_unit_test(
          test_public_bound_lease_methods_cover_state_and_attachments),
      cmocka_unit_test(
          test_public_lease_attach_rejects_malformed_json_response),
      cmocka_unit_test(
          test_public_lease_attach_retries_node_passive_and_cleans_parser_state),
      cmocka_unit_test(
          test_queue_transport_retries_node_passive_and_cleans_parser_state),
      cmocka_unit_test(
          test_enqueue_from_retries_node_passive_and_cleans_parser_state),
      cmocka_unit_test(test_public_lease_save_uses_mapped_lonejson_upload),
      cmocka_unit_test(
          test_public_lease_load_respects_configured_json_response_limit),
      cmocka_unit_test(test_public_lease_mutate_local_covers_no_content_path),
      cmocka_unit_test(test_public_management_methods_emit_logs),
      cmocka_unit_test(test_public_enqueue_emits_logs),
      cmocka_unit_test(test_public_dequeue_emits_stream_transport_logs),
      cmocka_unit_test(test_public_query_stream_captures_headers_and_body),
      cmocka_unit_test(test_public_query_stream_rejects_invalid_index_seq),
      cmocka_unit_test(test_public_queue_nack_maps_enum_intents),
      cmocka_unit_test(test_public_queue_nack_rejects_invalid_intent),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
