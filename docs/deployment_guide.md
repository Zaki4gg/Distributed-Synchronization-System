## `docs/deployment_guide.md`

```markdown
# Deployment Guide

Panduan ini mencakup **prasyarat**, **konfigurasi environment**, **cara menjalankan**, **verifikasi**, **skala**, **testing**, dan **troubleshooting** di Windows (PowerShell). Proses di Linux/Mac serupa.

---

## 1) Prasyarat
- **Docker Desktop** / Docker Engine + Docker Compose
- **Python 3.10+** (untuk menjalankan tes & Locust)
- `pip install -r requirements.txt`
  - Termasuk: `fastapi`, `uvicorn`, `requests`, `tenacity`, `locust`, dst.

---

## 2) Konfigurasi Environment
Salin `.env.example` menjadi `.env`, lalu sesuaikan variabel jika perlu. Variabel umum:

contoh

REDIS_URL=redis://redis:6379/0
LOCK_HTTP_PORT_BASE=8080
QUEUE_HTTP_PORT_BASE=8180
CACHE_HTTP_PORT_BASE=8280
QUEUE_REPLICATION_FACTOR=2
CACHE_CAPACITY=512


---

## 3) Build & Orchestration
Jalankan semua komponen via Compose:
```powershell
docker compose -f .\docker\docker-compose.yml up -d --build


Verifikasi awal:

curl.exe -s http://localhost:8081/raft/leader      # role=leader
curl.exe -s http://localhost:8180/readyz           # {"ready":true}
curl.exe -s http://localhost:8280/cache/metrics    # metrics JSON


Catatan Windows & PowerShell
Untuk POST JSON, lebih aman gunakan Invoke-RestMethod -Body ( @{...} | ConvertTo-Json -Compress ) dibanding curl dengan kutip manual—untuk menghindari JSON decode error.

4) Uji Fungsi Dasar
4.1 Lock (exclusive → state → release)
$L="http://localhost:8081"
Invoke-RestMethod -Method Post -Uri "$L/lock/acquire" -ContentType 'application/json' -Body (@{resource="demo";mode="exclusive";client_id="cli1";timeout_ms=2000}|ConvertTo-Json -Compress)
curl.exe -s "$L/lock/state?resource=demo"
Invoke-RestMethod -Method Post -Uri "$L/lock/release" -ContentType 'application/json' -Body (@{resource="demo";token="t1"}|ConvertTo-Json -Compress)

4.2 Queue (publish → consume → ack_owner)
1..5 | % { Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/publish?topic=alpha" -ContentType 'application/json' -Body (@{i=$_}|ConvertTo-Json -Compress) }
$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8182/queue/consume?topic=alpha&visibility_ttl=30000&max=10"
$r | % { Invoke-RestMethod -Method Post -Uri ("http://localhost:8182/queue/ack_owner?topic=alpha&owner={0}&msg_id={1}" -f $_.owner,$_.msg_id) }

4.3 Cache (put → get → invalidate)
Invoke-RestMethod -Method Post -Uri "http://localhost:8280/cache/put" -ContentType 'application/json' -Body (@{key="k1";value="v1"}|ConvertTo-Json -Compress)
curl.exe -s "http://localhost:8281/cache/get?key=k1"      # hit:true, v1
Invoke-RestMethod -Method Post -Uri "http://localhost:8282/cache/put" -ContentType 'application/json' -Body (@{key="k1";value="v2"}|ConvertTo-Json -Compress)
curl.exe -s "http://localhost:8280/cache/get?key=k1"      # v2
'8280','8281','8282' | % { "$_ => " + (curl.exe -s "http://localhost:$_/cache/state?key=k1") }

5) Scaling

Skala komponen (misal queue) dengan:

docker compose -f .\docker\docker-compose.yml up -d --scale queue=3


Setiap replika akan otomatis ikut ring consistent hashing.

6) Integration & Performance Testing
6.1 Pytest (unit/integration/performance ringan)

Pastikan PYTHONPATH menunjuk ke src/ saat menjalankan tes di host:

$env:PYTHONPATH="$PWD\src"
python -m pytest -q tests/unit/test_lock_manager.py
python -m pytest -q tests/integration/test_queue_end_to_end.py
python -m pytest -q tests/performance/test_cache_perf.py

6.2 Locust (UI dan Headless)

UI:

$env:QUEUE_HOST="http://localhost:8182"
$env:CACHE_HOST="http://localhost:8280"
$env:LOCK_HOST="http://localhost:8081"
$env:QUEUE_WEIGHT="3"; $env:CACHE_WEIGHT="2"; $env:LOCK_WEIGHT="1"
locust -f benchmarks\load_test_scenarios.py


Buka http://localhost:8089, set Users=120, Spawn rate=30, Start → amati RPS, Failures/s, p95.

Headless + CSV:

locust -f benchmarks\load_test_scenarios.py --headless -u 120 -r 30 -t 2m --csv bench_all


Gunakan bench_all_stats.csv untuk grafik di laporan.

7) Operasi & Maintenance

Logs: docker compose logs -f <service>

Rolling update: rebuild image → docker compose up -d --no-deps --build <service>

Backup: volume Redis/dirs data (jika ada) disertakan dalam backup.

8) Troubleshooting

Q: JSON decode error saat POST di PowerShell
A: Gunakan Invoke-RestMethod + ConvertTo-Json -Compress, atau simpan payload ke file *.json lalu --data-binary "@file.json".

Q: Simulasi network partition
A: Putuskan jaringan container target:

$cid = docker compose -f .\docker\docker-compose.yml ps -q lock2
$net = (docker inspect -f "{{json .NetworkSettings.Networks}}" $cid | ConvertFrom-Json).psobject.Properties.Name | Select-Object -First 1
docker network disconnect $net $cid
# kembalikan:
docker network connect $net $cid


Q: ACK gagal not_in_inflight
A: Pastikan ACK dikirim ke owner yang tepat (pakai /queue/owners atau field owner dari hasil consume). Gunakan ack_owner atau langsung ack ke http://localhost:<port-owner>.

Q: Cache tidak konsisten setelah write
A: Cek /cache/metrics → inv_sent/inv_recv harus bertambah. Pastikan semua node reachable.

9) Keamanan (Roadmap)

mTLS antar node, rotasi sertifikat.

RBAC untuk endpoint admin (mis. internal routes).

Audit logging untuk operasi sensitif.

10) Clean Up
docker compose -f .\docker\docker-compose.yml down -v


Membersihkan container, network, dan volume terkait (hati-hati: data hilang).