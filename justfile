clean:
    cargo clean

# for macos
develop:
    LIBCLANG_PATH=${HOMEBREW_PREFIX}/opt/llvm/lib \
    CC=${HOMEBREW_PREFIX}/opt/llvm/bin/clang \
    CXX=${HOMEBREW_PREFIX}/opt/llvm/bin/clang++ \
    AR=${HOMEBREW_PREFIX}/opt/llvm/bin/llvm-ar \
    CFLAGS="-flto=thin -O3" \
    CXXFLAGS="-flto=thin -O3" \
    RUSTFLAGS="-Clinker-plugin-lto -Clinker=$PWD/macos-linker.sh -Clink-arg=-fuse-ld=${HOMEBREW_PREFIX}/opt/llvm/bin/ld64.lld" \
    maturin develop --release --verbose

# for macos
build:
    LIBCLANG_PATH=${HOMEBREW_PREFIX}/opt/llvm/lib \
    CC=${HOMEBREW_PREFIX}/opt/llvm/bin/clang \
    CXX=${HOMEBREW_PREFIX}/opt/llvm/bin/clang++ \
    AR=${HOMEBREW_PREFIX}/opt/llvm/bin/llvm-ar \
    CFLAGS="-flto=thin -O3" \
    CXXFLAGS="-flto=thin -O3" \
    RUSTFLAGS="-Clinker-plugin-lto -Clinker=$PWD/macos-linker.sh -Clink-arg=-fuse-ld=${HOMEBREW_PREFIX}/opt/llvm/bin/ld64.lld" \
    maturin build --release --verbose
