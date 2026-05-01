local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local owner = os.getenv("LOCKDC_OWNER") or "tests-lua-bundle-sources"
local key_prefix = os.getenv("LOCKDC_KEY_PREFIX") or "tests/lua/bundle-sources"

local function read_file(path)
  local file = assert(io.open(path, "rb"))
  local bytes = assert(file:read("*a"))

  file:close()
  return bytes
end

local bundle_bytes = read_file(client_pem)

local variants = {
  {
    name = "path",
    source = function()
      return { path = client_pem }
    end,
  },
  {
    name = "string",
    source = function()
      return bundle_bytes
    end,
  },
  {
    name = "bytes",
    source = function()
      return { bytes = bundle_bytes }
    end,
  },
  {
    name = "callback",
    source = function()
      local offset = 1
      local read_calls = 0
      local closed = false
      local max_chunk = 17

      return {
        read = function(max_bytes)
          local remaining = #bundle_bytes - offset + 1
          local chunk_len

          read_calls = read_calls + 1
          if remaining <= 0 then
            return nil
          end
          chunk_len = math.min(remaining, max_bytes, max_chunk)
          local chunk = bundle_bytes:sub(offset, offset + chunk_len - 1)
          offset = offset + chunk_len
          return chunk
        end,
        close = function()
          closed = true
        end,
        assert_after_open = function()
          if read_calls <= 1 then
            error(("callback source was not read in chunks; read_calls=%d"):format(read_calls))
          end
          if not closed then
            error("callback source close callback was not invoked")
          end
        end,
      }
    end,
  },
}

for index, variant in ipairs(variants) do
  local source = variant.source()
  local after_open = source.assert_after_open
  local client, err = lockdc.open({
    endpoints = { endpoint },
    client_bundle_source = source,
    default_namespace = namespace_name,
  })

  if client == nil then
    error(("lockdc.open failed for %s bundle source: %s"):format(variant.name, err and err.message or tostring(err)))
  end
  if after_open ~= nil then
    after_open()
  end

  local key = ("%s/%s/%d/%d"):format(key_prefix, variant.name, os.time(), index)
  local lease, acquire_err = client:acquire({
    namespace_name = namespace_name,
    key = key,
    owner = owner .. "-" .. variant.name,
    ttl_seconds = 60,
    if_not_exists = true,
  })

  if lease == nil then
    client:close()
    error(("client:acquire failed for %s bundle source: %s"):format(variant.name, acquire_err and acquire_err.message or tostring(acquire_err)))
  end

  local update_res, update_err = lease:update_json({
    source = variant.name,
    index = index,
  })

  if update_res == nil then
    lease:close()
    client:close()
    error(("lease:update_json failed for %s bundle source: %s"):format(variant.name, update_err and update_err.message or tostring(update_err)))
  end

  local document, get_err = lease:get_json()
  if document == nil then
    lease:close()
    client:close()
    error(("lease:get_json failed for %s bundle source: %s"):format(variant.name, get_err and get_err.message or tostring(get_err)))
  end
  if type(document) ~= "table" or document.source ~= variant.name or document.index ~= index then
    lease:close()
    client:close()
    error(("unexpected get_json payload for %s bundle source"):format(variant.name))
  end

  lease:close()
  client:close()
end
