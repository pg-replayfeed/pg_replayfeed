# pg_replayfeed Makefile (PGXS)
#
# Layout:
#   src/     C sources — build artifacts never land here
#   build/   object files (created by `make`, removed by `make clean`)
#   sql/     extension SQL scripts
#   test/    pg_regress suites (`make test`)
#   bench/   end-to-end benchmarks (`make bench`)
#   e2e/     Go end-to-end: 1 producer + N consumers (`make e2e`)
#   scripts/ throwaway-instance helpers (`make run`)

MODULE_big = pg_replayfeed

SRCDIR   = src
BUILDDIR = build

SRCS = $(SRCDIR)/pg_replayfeed.c \
       $(SRCDIR)/catalog.c \
       $(SRCDIR)/feed.c \
       $(SRCDIR)/decode.c \
       $(SRCDIR)/extract.c \
       $(SRCDIR)/encode.c
OBJS = $(patsubst $(SRCDIR)/%.c,$(BUILDDIR)/%.o,$(SRCS))

EXTENSION = pg_replayfeed
DATA = sql/pg_replayfeed--2.0.sql sql/pg_replayfeed--1.1--2.0.sql

# pg_regress suites (slot-based streaming tests)
REGRESS = basic txn datatypes ddl retention security
REGRESS_OPTS = --inputdir=test --outputdir=test/output

# Isolation specs are retired in 2.0: cursor allocation, snapshot fences and
# prune races were properties of the trigger/event-table design. The WAL
# stream gets its ordering and atomicity from logical decoding itself.

# Disable LLVM bitcode generation: logical decoding output plugins are pure C
# shared libraries (.so / .dylib) loaded by walsender, not expression JIT modules.
# When PostgreSQL is installed via distribution packages (e.g. Debian/Ubuntu/Homebrew),
# the specific clang version hardcoded in Makefile.global (e.g. clang-19) is often
# not installed, causing build failures.
override with_llvm = no

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Apple Clang does not support -fexcess-precision=standard (often present in
# Homebrew PostgreSQL CFLAGS) and warns about it. With -Werror, this breaks compilation.
ifeq ($(shell uname -s),Darwin)
  override CFLAGS += -Wno-ignored-optimization-argument -Wno-unknown-warning-option
endif

# --- objects go to $(BUILDDIR)/, never next to the sources -------------------
# More specific than PGXS's own `%.o : %.c` (shorter stem wins), so make picks
# this rule for build/*.o. The recipe mirrors PGXS's compile line; CFLAGS and
# CPPFLAGS already carry PGXS's -fPIC and server -I flags when it expands.
$(BUILDDIR)/%.o: $(SRCDIR)/%.c | $(BUILDDIR)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

# PGXS `clean` removes $(OBJS) (= build/*.o); also drop the build dir itself
# and any stale objects left in src/ by the pre-build/ layout.
clean: clean-build-dir
clean-build-dir:
	rm -rf $(BUILDDIR)
	rm -f $(SRCDIR)/*.o $(SRCDIR)/*.bc

# --- developer targets --------------------------------------------------------
.PHONY: test run bench bench-dedicated e2e clean-build-dir producer consumer loadtest

# Full suite on a throwaway instance (installs first).
test: install
	bash test/run_tests.sh

# Throwaway instance with the extension installed + interactive psql.
# Exiting psql tears the instance down.
run: install
	bash scripts/dev_instance.sh

# End-to-end benchmarks on a throwaway instance (installs first).
bench: install
	bash bench/bench.sh

# Dedicated long-running sustained benchmark matrix with latency percentiles and lag.
bench-dedicated: install
	bash bench/bench_dedicated.sh

# Go end-to-end: continuous producer + N consumers on a throwaway instance
# (installs first). Ctrl-C stops and verifies. E2E_CONSUMERS / E2E_RATE tune
# the run; E2E_OPS=<n> switches to a finite deterministic mode.
e2e: install
	bash e2e/run_e2e.sh

# Interactive demo on a persistent instance (~/.pg_replayfeed_dev, kept across
# restarts): `make producer` ticks 1 op/sec; `make consumer` tails the feed
# live and resumes from its saved cursor on restart.
producer: install
	bash e2e/run_producer.sh

consumer: install
	bash e2e/run_consumer.sh

# Load test: writers + streaming consumers; end-to-end lag percentiles.
# Tune with LOAD_WRITERS / LOAD_CONSUMERS / LOAD_DURATION (e.g. 60s).
loadtest: install
	bash e2e/run_loadtest.sh

# Grafana + Prometheus monitoring stack for real-time p99 lag and characteristics
grafana-up:
	docker compose -f docker-compose.grafana.yml up -d
	@echo "Grafana listening at http://localhost:3000 (pre-loaded with pg_replayfeed p99 lag dashboard)"

grafana-down:
	docker compose -f docker-compose.grafana.yml down
