# Arsitektur Sistem – Distributed Synchronization System

## 1. Gambaran Umum
Proyek ini terdiri dari tiga layanan utama yang berjalan terdistribusi dan diorkestrasi oleh Docker Compose:

- **Distributed Lock Manager (DLM)** — 3 node (`:8080–:8082`) menggunakan **Raft** untuk mereplikasi log operasi lock. Mendukung **shared** & **exclusive** locks, **deadlock detection** sederhana (circular-wait/timeout), serta tahan **network partition** (selama quorum ≥2).
- **Distributed Queue** — 3 node (`:8180–:8182`) dengan **consistent hashing** pada pasangan `(topic, key)` untuk menentukan **owner shard**. Menyediakan **at-least-once delivery** via **visibility TTL** + **ACK**, serta **persistence & recovery** (pesan tetap aman saat owner down).
- **Distributed Cache** — 3 node (`:8280–:8282`) dengan protokol koherensi **MESI**. Operasi write mengirim **invalidation** ke peer. Mendukung **TTL per key** dan **LRU eviction**; expose **metrics** (hits, misses, evictions, invalidation) per node.

Komponen pendukung:
- **Redis** sebagai penyimpanan state/persisten untuk queue dan/atau koordinasi ringan.
- **Metrics endpoint** pada tiap layanan untuk observabilitas.
- **Locust** untuk uji beban terpadu (queue/cache/lock).

## 2. Diagram 
                           ┌───────────────────────────────────────────┐
                           │                 Redis                    │
                           │  Queue persistence + cache metadata      │
                           └───────────────┬───────────────┬──────────┘
                                           │               │
                                           │               │
        ┌──────────────────────┐     ⇆     ┌──────────────────────┐     ⇆     ┌──────────────────────┐
        │        Node 1        │           │        Node 2        │           │        Node 3        │
        │  lock1 / queue1 /    │           │  lock2 / queue2 /    │           │  lock3 / queue3 /    │
        │  cache1              │           │  cache2              │           │  cache3              │
        │  (Raft Follower)     │           │  (Raft Follower)     │           │  (Raft Leader)       │
        │                      │           │                      │           │                      │
        └───────────┬──────────┘           └───────────┬──────────┘           └───────────┬──────────┘
                    │                                  │                                  │
                    └────────────── REST API (Clients & Users) ───────────────────────────┘

   Catatan:
   • Garis ⇆ antar node = komunikasi antarnode:
     - Raft (Lock): heartbeat & log replication leader↔followers
     - Queue: routing shard (consistent hashing) & ack_owner
     - Cache: invalidation MESI (M/E/S/I) antar replika
   • Redis: penyimpanan pesan queue yang persisten + metadata cache (opsional)
