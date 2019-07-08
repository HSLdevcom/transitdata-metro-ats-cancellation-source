package fi.hsl.transitdata.metro.ats.models;

import fi.hsl.common.transitdata.proto.InternalMessages;

public class CancellationData {
    private InternalMessages.TripCancellation payload;
    private long timestampEpochMs;
    private String dvjId;

    public CancellationData(InternalMessages.TripCancellation payload, long timestampEpochMs, String dvjId) {
        this.payload = payload;
        this.timestampEpochMs = timestampEpochMs;
        this.dvjId = dvjId;
    }

    public String getDvjId() {
        return dvjId;
    }

    public InternalMessages.TripCancellation getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestampEpochMs;
    }
}
