"""Audio normalization (PyAV) and Opus encoding (opuslib).

Responsible for taking raw audio from any of the three ingestion sources
(each potentially at a different sample rate / channel layout / container)
and producing a uniform stream of Opus frames ready for RFC 2198 wrapping.
"""

from __future__ import annotations

import logging

import av
import numpy as np
import opuslib

from src.rfc2198 import wrap_rfc2198

logger = logging.getLogger(__name__)

TARGET_SAMPLE_RATE = 48_000
TARGET_CHANNELS = 1
OPUS_FRAME_MS = 20
OPUS_FRAME_SAMPLES = TARGET_SAMPLE_RATE * OPUS_FRAME_MS // 1000


class AudioNormalizer:
    """Wraps a PyAV resampler to coerce arbitrary input audio to the
    target sample rate / channel layout / sample format used by the
    Opus encoder.
    """

    def __init__(
        self,
        target_rate: int = TARGET_SAMPLE_RATE,
        target_channels: int = TARGET_CHANNELS,
    ) -> None:
        self._target_rate = target_rate
        self._target_channels = target_channels
        self._resampler = av.AudioResampler(
            format="s16",
            layout="mono" if target_channels == 1 else "stereo",
            rate=target_rate,
        )

    def normalize(self, frame: av.AudioFrame) -> list[av.AudioFrame]:
        """Resample/reformat a raw PyAV frame to the target format."""
        return self._resampler.resample(frame)


class OpusEncoder:
    """Thin wrapper around opuslib.Encoder producing fixed-size Opus frames."""

    def __init__(
        self,
        sample_rate: int = TARGET_SAMPLE_RATE,
        channels: int = TARGET_CHANNELS,
        application: int = opuslib.APPLICATION_VOIP,
    ) -> None:
        self._encoder = opuslib.Encoder(sample_rate, channels, application)
        self._frame_samples = sample_rate * OPUS_FRAME_MS // 1000

    def encode(self, pcm: np.ndarray) -> bytes:
        """Encode a single frame of int16 PCM samples to an Opus packet.

        `pcm` must contain exactly `self._frame_samples` samples per channel.
        """
        if pcm.dtype != np.int16:
            raise ValueError("pcm must be int16")
        return self._encoder.encode(pcm.tobytes(), self._frame_samples)


class AudioPipeline:
    """Normalizes, encodes, and RFC 2198-wraps one ingestion stream.

    Owns all per-stream state: the resampler, the Opus encoder, a buffer for
    PCM that hasn't yet accumulated into a full 20ms frame, and the previous
    Opus frame carried as redundancy. One instance per connection/source --
    state must not be shared across streams.
    """

    def __init__(
        self,
        source_rate: int = TARGET_SAMPLE_RATE,
        source_channels: int = TARGET_CHANNELS,
    ) -> None:
        self._source_rate = source_rate
        self._source_channels = source_channels
        self._layout = "mono" if source_channels == 1 else "stereo"
        self._normalizer = AudioNormalizer()
        self._encoder = OpusEncoder()
        self._previous_opus: bytes | None = None
        self._pcm_buffer = np.empty(0, dtype=np.int16)

    def process_chunk(self, raw_audio_bytes: bytes) -> list[bytes]:
        """Normalize + encode + RFC 2198-wrap one chunk of raw PCM audio.

        `raw_audio_bytes` is interleaved int16 PCM at `source_rate` /
        `source_channels`. Any samples left over after the last full 20ms
        Opus frame are buffered and prepended to the next call, so the
        returned list may be empty (chunk too small) or contain several
        packets (chunk spans multiple 20ms frames).
        """
        samples = np.frombuffer(raw_audio_bytes, dtype=np.int16)
        samples = samples.reshape(1, -1) if self._source_channels == 1 else samples.reshape(-1, self._source_channels).T

        raw_frame = av.AudioFrame.from_ndarray(samples, format="s16", layout=self._layout)
        raw_frame.sample_rate = self._source_rate

        for resampled in self._normalizer.normalize(raw_frame):
            pcm = resampled.to_ndarray().reshape(-1).astype(np.int16)
            self._pcm_buffer = np.concatenate([self._pcm_buffer, pcm])

        packets: list[bytes] = []
        while len(self._pcm_buffer) >= OPUS_FRAME_SAMPLES:
            frame, self._pcm_buffer = (
                self._pcm_buffer[:OPUS_FRAME_SAMPLES],
                self._pcm_buffer[OPUS_FRAME_SAMPLES:],
            )
            opus_packet = self._encoder.encode(frame)
            packets.append(wrap_rfc2198(opus_packet, self._previous_opus))
            self._previous_opus = opus_packet

        return packets
