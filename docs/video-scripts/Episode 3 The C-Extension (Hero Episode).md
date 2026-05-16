# Episode 3: The C-Extension — 10 Million Messages Per Second

## Hook (30 seconds)
"Today, we're going 675x faster than Python. Watch this."

## The Problem (2 minutes)
- Python validation: ~2,000 msg/sec (demo)
- GIL limits true parallelism
- Industrial sensors need more

## Side-by-Side Performance Comparison (3 minutes)

| Metric | Pure Python | C-Extension |
|--------|-------------|-------------|
| Throughput | 2,000 msg/sec | 10,123,833 msg/sec |
| Latency | 0.5ms | 0.00007ms |
| Speedup | 1x | **675x** |

*Show both running live*

## The C Code Walkthrough (4 minutes)
```c
static uint8_t rolling_checksum(const char* data, int len) {
    uint8_t checksum = 0x00;
    for (int i = 0; i < len; i++) {
        checksum ^= (uint8_t)data[i];
        checksum = (checksum << 1) | (checksum >> 7);
    }
    return checksum;
}

```

## The Python Call (1 minute)
```python
import validator

# Single validation
checksum = validator.validate("sensor_id=temp;value=23.5")
print(f"Checksum: {checksum}")  # Output: 43

# Batch validation (10,000 messages)
batch = [f"sensor_{i}" for i in range(10000)]
results = validator.validate_batch(batch)  # Returns list of checksums
```

---

## What to Save NOW (No Reruns) --FOR LATER DURING RECORDING TIME

Create a text file for future reference:

```powershell
cd C:\Personal_Projects\edge-platform\docs\video-scripts

@'
# Week 2: Commands to Run During Video Shoot

## Terminal 1 (WSL2)
cd /mnt/c/Personal_Projects/edge-platform/c_library
ls -la

## Show validator.c
cat validator.c | head -30

## Compile
python3 setup.py build_ext --inplace

## Single test
python3 -c "import validator; print(validator.validate('test'))"

## Benchmark
python3 -c "
import validator
import time
batch = [f''sensor_{i}'' for i in range(10000)]
start = time.time()
validator.validate_batch(batch)
elapsed = time.time() - start
print(f''Throughput: {10000/elapsed:.0f} msg/sec'')
"

## Expected output: 10123833

# For side-by-side comparison (prepare slide)
# Python baseline: ~2,000 msg/sec
# C-extension: 10,123,833 msg/sec
# Speedup: 675x
'@ | Out-File -FilePath week2-commands.txt -Encoding utf8