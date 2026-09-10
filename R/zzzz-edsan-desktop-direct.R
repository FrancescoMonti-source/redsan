# Desktop EDSaN CT direct connection -----------------------------------------
#
# d2imr's proxy configuration is specific to Podsan and may point to the
# internal `edsan-squid` host, which is not resolvable from DIM Windows
# workstations. Interactive desktop calls already target EDSaN CT directly, so
# explicitly disable proxy discovery/configuration for that path. The standard
# keystore-backed d2imr path is unchanged and continues to use d2imr's own proxy
# policy.

.edsan_ct_proxy_config <- function() {
  if (!requireNamespace("httr", quietly = TRUE)) return(NULL)

  proxy <- getOption("redsan.edsan_ct_proxy", NULL)
  if (.edsan_ct_valid_scalar(proxy)) {
    return(httr::use_proxy(proxy))
  }

  # An empty CURLOPT_PROXY explicitly bypasses proxy settings inherited from
  # d2imr or the process environment.
  httr::config(proxy = "")
}
