#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET="${REMOTE_HOSTS_WINDOWS_TARGET:-x86_64-pc-windows-msvc}"
DIST_DIR="${REMOTE_HOSTS_DIST_DIR:-${ROOT_DIR}/dist}"
MINIMUM_XWIN_VERSION="0.23.0"

require_command() {
  local command_name="$1"
  local hint="$2"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "missing required command '${command_name}': ${hint}" >&2
    exit 1
  fi
}

version_at_least() {
  local actual="$1"
  local minimum="$2"
  awk -v actual="${actual}" -v minimum="${minimum}" '
    BEGIN {
      split(actual, a, "."); split(minimum, m, ".")
      for (i = 1; i <= 3; i++) {
        av = a[i] + 0; mv = m[i] + 0
        if (av > mv) exit 0
        if (av < mv) exit 1
      }
      exit 0
    }
  '
}

require_command cargo "install Rust with rustup"
require_command rustup "install Rust with rustup"
require_command cargo-xwin "cargo install cargo-xwin --version ${MINIMUM_XWIN_VERSION} --locked"
require_command zip "install a ZIP utility"
require_command shasum "install a SHA-256 utility"

if [[ "${TARGET}" == "x86_64-pc-windows-msvc" ]]; then
  require_command nasm "macOS: brew install nasm; Debian/Ubuntu: apt install nasm"
fi

xwin_version="$(cargo xwin --version | awk '{print $2}')"
if ! version_at_least "${xwin_version}" "${MINIMUM_XWIN_VERSION}"; then
  echo "cargo-xwin ${xwin_version} is too old; install ${MINIMUM_XWIN_VERSION} or newer" >&2
  exit 1
fi

rustup target add "${TARGET}"

cd "${ROOT_DIR}"
cargo xwin build \
  --locked \
  --release \
  --target "${TARGET}" \
  -p remote-hosts-cli \
  -p remote-hosts-launcher \
  --bins

target_dir="$(cargo metadata --no-deps --format-version=1 \
  | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
if [[ -z "${target_dir}" ]]; then
  echo "could not resolve Cargo target directory" >&2
  exit 1
fi

binary_path="${target_dir}/${TARGET}/release/remote-hosts.exe"
launcher_path="${target_dir}/${TARGET}/release/remote-hosts-launcher.exe"
if [[ ! -f "${binary_path}" ]]; then
  echo "Windows binary was not produced at ${binary_path}" >&2
  exit 1
fi
if [[ ! -f "${launcher_path}" ]]; then
  echo "Windows launcher was not produced at ${launcher_path}" >&2
  exit 1
fi

version="$(awk -F '"' '$1 ~ /^version = / { print $2; exit }' crates/remote-hosts-cli/Cargo.toml)"
commit="$(git rev-parse --short=12 HEAD 2>/dev/null || printf 'source')"
built_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
package_name="remote-hosts-windows-x86_64-${version}-${commit}"
stage_dir="${DIST_DIR}/${package_name}"
zip_path="${DIST_DIR}/${package_name}.zip"
checksum_path="${zip_path}.sha256"

rm -rf "${stage_dir}"
rm -f "${zip_path}" "${checksum_path}"
mkdir -p "${stage_dir}/skills"

install -m 0644 "${binary_path}" "${stage_dir}/remote-hosts.exe"
install -m 0644 "${launcher_path}" "${stage_dir}/remote-hosts-launcher.exe"
install -m 0644 scripts/remote-hosts-service.ps1 "${stage_dir}/remote-hosts-service.ps1"
install -m 0644 crates/remote-hosts-api/src/admin.html "${stage_dir}/admin.html"
install -m 0644 docs/windows.md "${stage_dir}/README-WINDOWS.md"
cp -R skills/remote-hosts-agent "${stage_dir}/skills/remote-hosts-agent"

cat >"${stage_dir}/release.json" <<EOF
{
  "version": "${version}",
  "commit": "${commit}",
  "target": "${TARGET}",
  "built_at": "${built_at}",
  "cargo_xwin": "${xwin_version}"
}
EOF

(
  cd "${DIST_DIR}"
  zip -q -r "$(basename "${zip_path}")" "${package_name}"
)

checksum="$(shasum -a 256 "${zip_path}" | awk '{print $1}')"
printf '%s  %s\n' "${checksum}" "$(basename "${zip_path}")" >"${checksum_path}"

cat <<EOF
Windows package: ${zip_path}
SHA-256:         ${checksum}
Manifest:        ${stage_dir}/release.json
EOF
