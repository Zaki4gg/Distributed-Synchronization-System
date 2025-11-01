# Link Youtube 
`https://youtu.be/YATuvDxKNw4`
# Link report pdf 
`https://drive.google.com/file/d/1Ara1CyAXM4EHJcqSCaPtLg1y_531WsLa/view?usp=sharing`

# Distributed Synchronization System — README
Lock (Raft) · Queue (Consistent Hashing + At-Least-Once) · Cache (MESI + LRU) — 3 node, Dockerized

## Fitur Utama (sesuai Core Requirements)
### Distributed Lock Manager (Raft)
  3 node, leader election, write hanya di leader (followers redirect)
  <b>Shared & Exclusive locks, deadlock detection sederhana (wait-for + timeout)
  <b>Network partition → hanya quorum yang boleh commit
### Distributed Queue System
  Consistent hashing untuk pemilikan shard (topic,key) → owners
  <br>Persistence (Redis) + recovery, multiple producers/consumers
  <br>At-least-once delivery (visibility TTL + ACK / ACK owner)
### Distributed Cache Coherence
  MESI (M/E/S/I) + invalidation antar node
  <br>LRU eviction, TTL per key
  <br>Metrics: hits, misses, puts, evictions, inv_sent/recv
### Containerization
  Dockerfile per komponen, docker-compose orchestration
  <br>.env untuk konfigurasi, scaling mudah (menambah replika)

## naikkan cluster (kalau belum)
`docker compose -f .\docker\docker-compose.yml up --build -d`

## lihat status container
`docker compose -f .\docker\docker-compose.yml ps`

## health & readiness
`'8080','8081','8082','8180','8181','8182','8280','8281','8282' | % {
  "$_ -> " + (curl.exe -s "http://localhost:$_/readyz")
}`

## helper cari leader lock (Raft)
`function Get-Leader {
  param([int[]]$Ports = @(8080,8081,8082))
  foreach($p in $Ports){
    try{
      $j = (curl.exe -s "http://localhost:$p/raft/leader" | ConvertFrom-Json)
      if ($j.role -eq "leader") { return "http://localhost:$p" }
    } catch { }
  }
  return $null
}`

# A. Distributed Lock Manager (Raft) 
## A1. Leader election & 3 node komunikasi
`curl.exe -s http://localhost:8080/raft/leader
curl.exe -s http://localhost:8081/raft/leader
curl.exe -s http://localhost:8082/raft/leader`


✅ Harapan: tepat 1 node role":"leader", 2 lainnya follower.

## A2. Shared & Exclusive locks
### exclusive (cli-1) pada resource file-1
`'{"resource":"file-1","mode":"exclusive","client_id":"cli-1","timeout_ms":2000}' |
  Set-Content .\acq_ex.json -Encoding ascii
$leader = Get-Leader
curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@acq_ex.json"`

### dua shared pada resource file-2
`'{"resource":"file-2","mode":"shared","client_id":"cli-2","timeout_ms":2000}' |
  Set-Content .\s1.json -Encoding ascii
'{"resource":"file-2","mode":"shared","client_id":"cli-3","timeout_ms":2000}' |
  Set-Content .\s2.json -Encoding ascii`

`curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@s1.json"
curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@s2.json"`

### Lihat state file-1 (exclusive) & file-2 (shared)
`curl.exe -s "$leader/lock/state?resource=file-1" | ConvertFrom-Json | ConvertTo-Json -Depth 10
curl.exe -s "$leader/lock/state?resource=file-2" | ConvertFrom-Json | ConvertTo-Json -Depth 10`

## A3. Queueing + batch grant
### ambil exclusive pada file-3
`'{"resource":"file-3","mode":"exclusive","client_id":"cli-1","timeout_ms":2000}' |
  Set-Content .\ex3.json -Encoding ascii
$resp = curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@ex3.json" | ConvertFrom-Json
$tok = $resp.token`

### dua permintaan shared -> harus "queued"
`'{"resource":"file-3","mode":"shared","client_id":"cli-2","timeout_ms":2000}' | Set-Content .\q1.json -Encoding ascii
'{"resource":"file-3","mode":"shared","client_id":"cli-3","timeout_ms":2000}' | Set-Content .\q2.json -Encoding ascii
curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@q1.json"
curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@q2.json"`

### release exclusive -> shared batch granted
`@{"resource"="file-3";"token"=$tok} | ConvertTo-Json -Compress | Set-Content .\rel3.json -Encoding ascii
curl.exe -s -X POST "$leader/lock/release" -H "Content-Type: application/json" --data-binary "@rel3.json"`

### cek: holders file-3 harus berisi cli-2 & cli-3 (mode=shared)
`curl.exe -s "$leader/lock/state?resource=file-3" | ConvertFrom-Json | ConvertTo-Json -Depth 10`

## A4. Deadlock detection (distributed environment)
### A pegang R1
`'{"resource":"R1","mode":"exclusive","client_id":"A","timeout_ms":2000}' | Set-Content .\a_r1.json -Encoding ascii
$tokA = (curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@a_r1.json" | ConvertFrom-Json).token`

### B pegang R2
`'{"resource":"R2","mode":"exclusive","client_id":"B","timeout_ms":2000}' | Set-Content .\b_r2.json -Encoding ascii
$tokB = (curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@b_r2.json" | ConvertFrom-Json).token`

### A minta R2 (queued), B minta R1 (queued) -> siklus
`'{"resource":"R2","mode":"exclusive","client_id":"A","timeout_ms":2000}' | Set-Content .\a_wait_r2.json -Encoding ascii
'{"resource":"R1","mode":"exclusive","client_id":"B","timeout_ms":2000}' | Set-Content .\b_wait_r1.json -Encoding ascii
curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@a_wait_r2.json"
curl.exe -s -X POST "$leader/lock/acquire" -H "Content-Type: application/json" --data-binary "@b_wait_r1.json"`

### probe deadlock
`curl.exe -s -X POST "$leader/lock/deadlock_probe"`

## A5. Network partition scenarios
### Ambil container ID lock2
`$cid = docker compose -f .\docker\docker-compose.yml ps -q lock2`

### Ambil nama network tempat lock2 terhubung (key dari Networks dict)
`$net = (
  docker inspect -f "{{json .NetworkSettings.Networks}}" $cid |
    ConvertFrom-Json
).psobject.Properties.Name | Select-Object -First 1`

`"$cid uses network: $net"`

### Putuskan lock2 dari network compose-nya
`docker network disconnect $net $cid`

### Cek leader berubah (re-election kalau lock2 sebelumnya leader)
`curl.exe -s http://localhost:8080/raft/leader
curl.exe -s http://localhost:8081/raft/leader
curl.exe -s http://localhost:8082/raft/leader`

### Sambungkan kembali
docker network connect $net $cid


# B. Distributed Queue System
Endpoint yang dipakai: /queue/publish, /queue/consume, /queue/ack.
(At-least-once via visibility TTL; persistence via Redis; multi-producer/consumer diuji paralel.)

## B1. Publish dari dua node, consume di node lain (ack_owner)
Perintah

### Publish 1..10 ke topic=alpha lewat queue1 & Publish 1..10 lewat queue2
`1..10 | % { Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/publish?topic=alpha" -ContentType 'application/json' -Body (@{i=$_}|ConvertTo-Json -Compress) }
1..10 | % { Invoke-RestMethod -Method Post -Uri "http://localhost:8181/queue/publish?topic=alpha" -ContentType 'application/json' -Body (@{j=$_}|ConvertTo-Json -Compress) }`

### Consume dari queue3 (owner bisa queue1/queue3 sesuai hashing)
`$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8182/queue/consume?topic=alpha&visibility_ttl=30000&max=50"
$r | Format-Table`
### ACK via ack_owner
`$r | % { Invoke-RestMethod -Method Post -Uri ("http://localhost:8182/queue/ack_owner?topic=alpha&owner={0}&msg_id={1}" -f $_.owner,$_.msg_id) }`


Narasi: “Consistent hashing menentukan owner. Walau saya consume di node lain, saya ACK ke pemilik (idempotent) via ack_owner.”
Ekspektasi: Data tampil dengan kolom owner; ACK sukses.

## B2. At-least-once (TTL expire → re-deliver)
Perintah

### Publish satu pesan
`$m = Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/publish?topic=retry" -ContentType 'application/json' -Body '{"n":1}'`
### Ambil tapi JANGAN di-ACK, TTL 3 detik
`$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/consume?topic=retry&visibility_ttl=3000&max=1"
Start-Sleep -Seconds 4`
### Muncul lagi
`Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/consume?topic=retry&visibility_ttl=3000&max=1"`


Narasi: “Tanpa ACK, setelah TTL habis pesan akan muncul lagi. Inilah at-least-once.”
Ekspektasi: Pesan yang sama ter-deliver ulang setelah ~3–4 detik.

## B3. Persistence & recovery (stop-start node)
Perintah

### Publish beberapa pesan persist
`1..5 | % { Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/publish?topic=persist" -ContentType 'application/json' -Body (@{n=$_}|ConvertTo-Json -Compress) }`

### Stop queue1 (pemilik sebagian pesan)
`docker compose -f .\docker\docker-compose.yml stop queue1`

### Coba consume dari node lain → tetap kosong (owner down)
`curl.exe -s -X POST "http://localhost:8182/queue/consume?topic=persist&visibility_ttl=5000&max=3"`

### Start lagi → pesan muncul kembali lalu ACK
`docker compose -f .\docker\docker-compose.yml start queue1
$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8180/queue/consume?topic=persist&visibility_ttl=5000&max=10"
$r | % { Invoke-RestMethod -Method Post -Uri ("http://localhost:8180/queue/ack?topic=persist&msg_id={0}" -f $_.msg_id) }`

## B4. Consistent hashing (CH) + (idealnya) R=2

### 1. Lihat owner R=2 untuk key=user42 (catat siapa primary/replica)
`curl.exe -s "http://localhost:8180/queue/owners?topic=alpha&key=user42"`

### 2. Publish 3 pesan baru ke topic=alpha, key=user42 (replicated ke 2 owner)
`1..3 | % {
  Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8180/queue/publish?topic=alpha&key=user42" `
    -ContentType 'application/json' `
    -Body (@{ n = $_ } | ConvertTo-Json -Compress)
}`

### 3. Matikan owner primary (kalau owners mengandung queue1, stop queue1)
`docker compose -f .\docker\docker-compose.yml stop queue1`

### 4. Consume dari replica (8182) — simpan ke $r dan tampilkan
`$r = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8182/queue/consume?topic=alpha&key=user42&visibility_ttl=5000&max=10"
$r`

### 5. ACK spesifik owner (tampilkan hasil ACK agar terlihat True)
`foreach ($m in @($r)) {
  $ownerEnc = [uri]::EscapeDataString($m.owner)
  $res = Invoke-RestMethod -Method Post `
    -Uri ("http://localhost:8182/queue/ack_owner?topic=alpha&owner={0}&msg_id={1}" -f $ownerEnc, $m.msg_id)
  "$($m.msg_id) -> acked=$($res.acked)"
}`

### 6. Tunggu > TTL dan pastikan tidak re-deliver
`Start-Sleep -Seconds 6
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8182/queue/consume?topic=alpha&key=user42&visibility_ttl=5000&max=10"`

`curl.exe -s -X POST "http://localhost:8182/queue/consume?topic=alpha&key=user42&visibility_ttl=5000&max=10"`
harus tampil [] kalau kosong

`$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8182/queue/consume?topic=alpha&key=user42&visibility_ttl=5000&max=10"
if ($null -eq $r) { "OK: empty (no messages)" } else { "Items: " + (@($r).Count) }`

# C) Distributed Cache (MESI, invalidation, TTL, LRU, metrics) (±2 menit)
## C1. Put, propagate (MESI → S/M) dan invalidation
Perintah

### Put k1=v1 di cache1
`Invoke-RestMethod -Method Post -Uri "http://localhost:8280/cache/put" -ContentType 'application/json' -Body (@{key="k1";value="v1"}|ConvertTo-Json -Compress)`

### Get dari cache2 & cache3 (harap hit & propagasi)
`curl.exe -s "http://localhost:8281/cache/get?key=k1"
curl.exe -s "http://localhost:8282/cache/get?key=k1"`

### Update k1=v2 di cache3 → invalidation ke peer
`Invoke-RestMethod -Method Post -Uri "http://localhost:8282/cache/put" -ContentType 'application/json' -Body (@{key="k1";value="v2"}|ConvertTo-Json -Compress)`

### Baca lagi dari cache1 & 2 → harus v2
`curl.exe -s "http://localhost:8280/cache/get?key=k1"
curl.exe -s "http://localhost:8281/cache/get?key=k1"`

### Periksa state & metrics
`'8280','8281','8282' | % { "$_ => " + (curl.exe -s "http://localhost:$_/cache/state?key=k1") }
'8280','8281','8282' | % { "$_ => " + (curl.exe -s "http://localhost:$_/cache/metrics") }`


Narasi: “Protokol MESI: saat write, owner menjadi M, peer invalidated ke I lalu read berikutnya jadi S. Metrics memperlihatkan hits/misses & invalidation counters.”
Ekspektasi: Value konsisten v2; state kombinasi M/S sesuai akses; metrics inv_sent/recv naik.

## C2. TTL & LRU eviction (ringkas)
Perintah

### TTL: 5s
`Invoke-RestMethod -Method Post -Uri "http://localhost:8280/cache/put?ttl_ms=5000" -ContentType 'application/json' -Body (@{key="temp";value="123"}|ConvertTo-Json -Compress)
Start-Sleep -Seconds 6
curl.exe -s "http://localhost:8281/cache/get?key=temp"   # miss`

### LRU: dorong banyak kunci
`1..700 | % { Invoke-RestMethod -Method Post -Uri "http://localhost:8280/cache/put" -ContentType 'application/json' -Body (@{key="k$_";value=$_}|ConvertTo-Json -Compress) }
curl.exe -s "http://localhost:8280/cache/metrics"`


# Testing dengan pytest
`python -m pytest -q tests/integration/test_queue_end_to_end.py
python -m pytest -q tests/performance/test_cache_perf.py`

# testing benchmarks\load test scenario dengan locust
`$env:QUEUE_HOST="http://localhost:8182"
$env:CACHE_HOST="http://localhost:8280"
$env:LOCK_HOST="http://localhost:8081"`

`locust -f .\benchmarks\load_test_scenarios.py`

lalu pencet link

src/
  nodes/ (base_node.py, lock_manager.py, queue_node.py, cache_node.py)
  consensus/ (raft.py, pbft.py optional)
  communication/ (message_passing.py, failure_detector.py)
  utils/ (config.py, metrics.py)
docker/ (Dockerfile.*, docker-compose.yml, .env.example)
tests/ (unit/, integration/, performance/)
benchmarks/ (load_test_scenarios.py, locustfile.py)
docs/ (architecture.md, api_spec.yaml, deployment_guide.md)
