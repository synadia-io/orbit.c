# Public headers go under nats-sysclient/ since their names (varz.h, ...) are generic.
orbit_add_library(nats_sysclient NATS_SYSCLIENT
  DEPS nats_extra
  HEADERS sysclient.h healthz.h statsz.h varz.h connz.h subsz.h jsz.h
  HEADER_SUBDIR nats-sysclient)
