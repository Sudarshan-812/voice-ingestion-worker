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
