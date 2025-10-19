# Why do we have this?

The only difference between this and the upstream port now is that we use `-DCMAKE_FIND_FRAMEWORK="NEVER"`
to avoid using frameworks on macOS.

Not sure why - feel free to experiment with removing this.
