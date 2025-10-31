# src/communication/failure_detector.py
from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

# Catatan singkat:
# - Implementasi ini mengikuti ide "Phi Accrual Failure Detector" (Hayashibara dkk.)
# - Inti: hitung phi = -log10(1 - CDF(delta, mean, stddev))
#   di mana delta = waktu sejak heartbeat terakhir.
# - Jika phi >= threshold (umum: 8.0), node dicurigai fail/unavailable.
#
# Integrasi tipikal:
#   from communication.failure_detector import PhiAccrualFailureDetector, periodic_probe
#   fd = PhiAccrualFailureDetector()
#   asyncio.create_task(periodic_probe(
#       peers={"lock2":"http://lock2:8081","lock3":"http://lock3:8082"},
#       fd=fd,
#       probe_func=lambda url: get_json(f"{url}/health"),  # dari communication.message_passing
#       interval=0.5,
#       timeout=1.0,
#       on_change=lambda pid, avail, phi: logger.info(f"{pid} -> {avail} (phi={phi:.2f})")
#   ))
#
# Kemudahan:
# - Tidak bergantung pada Redis, dll. Murni lokal per-proses.
# - record() dipanggil saat heartbeat/ping sukses.


@dataclass
class _Stats:
    last_ts_ms: Optional[float] = None
    n: int = 0
    mean_ms: float = 0.0
    m2: float = 0.0  # untuk var dengan Welford's algorithm
    available: bool = True  # status terakhir yang dilaporkan


class PhiAccrualFailureDetector:
    """
    Phi Accrual Failure Detector sederhana.

    Parameter penting:
    - min_interval_ms: interval minimum antar heartbeat yang dipertimbangkan (noise filter).
    - max_sample_size: jumlah maksimum sampel yang diperhitungkan (dengan decay Welford via faktor alpha).
      (Implementasi ini tidak menyimpan seluruh deque; gunakan EMA-like decay agar stabil.)
    - acceptable_heartbeat_pause_ms: toleransi jeda (ditambahkan ke delta agar tidak terlalu agresif).
    - min_std_deviation_ms: mencegah stddev terlalu kecil (div-by-zero/phi meledak).
    - phi_threshold_default: default ambang ketersediaan di is_available().
    """

    def __init__(
        self,
        *,
        min_interval_ms: float = 200.0,
        max_sample_size: int = 1000,
        acceptable_heartbeat_pause_ms: float = 0.0,
        min_std_deviation_ms: float = 100.0,
        phi_threshold_default: float = 8.0,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._stats: Dict[str, _Stats] = {}
        self.min_interval_ms = float(min_interval_ms)
        self.max_sample_size = int(max_sample_size)
        self.acceptable_pause_ms = float(acceptable_heartbeat_pause_ms)
        self.min_std_ms = float(min_std_deviation_ms)
        self.phi_threshold_default = float(phi_threshold_default)
        self._clock = monotonic_clock

        # Alpha untuk peluruhan kontribusi sampel lama (EMA-ish).
        # Nilai ~0.1..0.2 cukup stabil. Kita turunkan alpha sesuai banyaknya sampel.
        self._base_alpha = 0.2

    # ------------------------------
    # API publik
    # ------------------------------

    def record(self, node_id: str, ts_ms: Optional[float] = None) -> None:
        """
        Catat heartbeat yang diterima dari node_id pada timestamp tertentu (ms).
        Jika ts_ms None, gunakan clock monotonic.
        """
        now_ms = ts_ms if ts_ms is not None else (self._clock() * 1000.0)
        st = self._stats.get(node_id)
        if st is None:
            st = _Stats(last_ts_ms=now_ms)
            self._stats[node_id] = st
            return

        # Hitung interval sejak HB terakhir
        if st.last_ts_ms is None:
            st.last_ts_ms = now_ms
            return

        interval_ms = now_ms - st.last_ts_ms
        if interval_ms < self.min_interval_ms:
            # Terlalu cepat -> abaikan
            st.last_ts_ms = now_ms
            return

        # Update statistik dengan Welford + peluruhan (EMA-like)
        # Kita gunakan alpha yang menurun jika sampel masih sedikit (biar stabil).
        st.n = min(st.n + 1, self.max_sample_size)
        alpha = self._alpha_for_samples(st.n)

        # Update mean & variance (m2) dengan exponential moving approach
        # EMA untuk mean:
        delta = interval_ms - st.mean_ms
        new_mean = st.mean_ms + alpha * delta
        # Var (approx) pakai "exponential moving variance" pendekatan:
        # m2 di sini bukan strictly Welford; kita kira-kira var dengan EMA dari squared error.
        new_m2 = (1 - alpha) * st.m2 + alpha * (interval_ms - new_mean) ** 2

        st.mean_ms = new_mean if st.n > 1 else interval_ms
        st.m2 = new_m2 if st.n > 1 else (interval_ms ** 2) * 0.1  # seed kecil
        st.last_ts_ms = now_ms

    def phi(self, node_id: str, now_ms: Optional[float] = None) -> float:
        """
        Hitung nilai phi untuk node_id saat ini.
        Phi tinggi => makin dicurigai gagal.
        """
        st = self._stats.get(node_id)
        if st is None or st.last_ts_ms is None or st.n < 2:
            # Belum cukup data untuk curiga
            return 0.0

        now = now_ms if now_ms is not None else (self._clock() * 1000.0)
        delta_ms = max(0.0, now - st.last_ts_ms - self.acceptable_pause_ms)

        # Ambil mean/std dev
        mean_ms = max(1.0, st.mean_ms)
        # stddev dari m2 (EMA): std = sqrt(m2)
        std_ms = max(self.min_std_ms, math.sqrt(max(st.m2, 1e-9)))

        # Normal CDF: p = 1 - CDF(delta)
        # CDF untuk normal(μ, σ): 0.5 * (1 + erf((x-μ)/(σ√2)))
        p = 1.0 - _normal_cdf(delta_ms, mean_ms, std_ms)
        p = min(max(p, 1e-12), 1.0 - 1e-12)  # clamp
        phi_val = -math.log10(p)
        return float(phi_val)

    def is_available(
        self, node_id: str, *, now_ms: Optional[float] = None, threshold: Optional[float] = None
    ) -> bool:
        """True jika phi < threshold (default 8.0)."""
        thr = self.phi_threshold_default if threshold is None else float(threshold)
        return self.phi(node_id, now_ms=now_ms) < thr

    def last_heartbeat_ms(self, node_id: str) -> Optional[float]:
        st = self._stats.get(node_id)
        return st.last_ts_ms if st else None

    def ensure_node(self, node_id: str) -> None:
        if node_id not in self._stats:
            self._stats[node_id] = _Stats(last_ts_ms=None)

    def remove(self, node_id: str) -> None:
        self._stats.pop(node_id, None)

    def snapshot(self) -> Dict[str, dict]:
        """Untuk debug/metrics."""
        out: Dict[str, dict] = {}
        for k, st in self._stats.items():
            std_ms = math.sqrt(st.m2) if st.m2 > 0 else 0.0
            out[k] = {
                "n": st.n,
                "mean_ms": st.mean_ms,
                "std_ms": std_ms,
                "last_ts_ms": st.last_ts_ms,
            }
        return out

    # ------------------------------
    # Helpers
    # ------------------------------

    def _alpha_for_samples(self, n: int) -> float:
        # Buat sampel awal tidak terlalu "gelap": alpha naik perlahan
        # sampai mendekati _base_alpha.
        # n kecil -> alpha lebih besar (adapt cepat), n besar -> alpha ≈ base_alpha (stabil).
        if n <= 0:
            return self._base_alpha
        return min(self._base_alpha, 0.5 / max(1, n))


def _normal_cdf(x: float, mean: float, std: float) -> float:
    z = (x - mean) / std
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


# ======================================================================
# Utilitas probe periodik (opsional)
# ======================================================================

async def periodic_probe(
    *,
    peers: Dict[str, str],
    fd: PhiAccrualFailureDetector,
    probe_func: Callable[[str], "asyncio.Future[dict]"] | Callable[[str], "asyncio.Future[object]"],
    interval: float = 0.5,
    timeout: float = 1.0,
    threshold: float = 8.0,
    on_change: Optional[Callable[[str, bool, float], None]] = None,
) -> None:
    """
    Loop asinkron untuk mem-probe peers dan update failure detector.
    - peers: mapping {peer_id: base_url}
    - probe_func: coroutine(url) -> response (tidak perlu spesifik, cukup sukses bila tidak exception)
      Rekomendasi: gunakan get_json dari communication.message_passing dengan endpoint /health.
    - interval: jeda antar putaran probe.
    - timeout: timeout per probe (gunakan di probe_func jika mendukung).
    - threshold: ambang phi untuk available/unavailable.
    - on_change: callback(peer_id, is_available, phi) ketika status berubah.

    Catatan:
    - Fungsi ini tidak raise; ia akan jalan terus (cocok untuk background task).
    - Pastikan dipanggil via: asyncio.create_task(periodic_probe(...))
    """
    # Seed node list
    for pid in peers.keys():
        fd.ensure_node(pid)

    # Cache status terakhir agar bisa memicu on_change
    last_status: Dict[str, bool] = {pid: True for pid in peers.keys()}

    while True:
        t0 = time.monotonic()
        # Jalankan probe paralel
        async def _one(pid: str, url: str):
            try:
                # Jika probe_func tidak menangani timeout, kita bungkus di wait_for
                res = await asyncio.wait_for(probe_func(url), timeout=timeout)
                # Sukses: catat heartbeat
                fd.record(pid)
            except Exception:
                # Gagal: biarkan FD yang menilai berdasarkan phi
                pass

        await asyncio.gather(*[_one(pid, url) for pid, url in peers.items()], return_exceptions=True)

        # Cek perubahan status
        for pid in peers.keys():
            phi_val = fd.phi(pid)
            avail = phi_val < threshold
            prev = last_status.get(pid)
            if prev is None or prev != avail:
                last_status[pid] = avail
                if on_change:
                    try:
                        on_change(pid, avail, phi_val)
                    except Exception:
                        # Jangan biarkan callback men-downtime loop probe
                        pass

        # Tunggu sisa interval
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0.0, interval - elapsed))
