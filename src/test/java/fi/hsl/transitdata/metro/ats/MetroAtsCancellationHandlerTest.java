package fi.hsl.transitdata.metro.ats;

import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetroAtsCancellationHandlerTest {
    /*
    @Test
    public void testFilteringWithEmptyList() {
        List<MetroAtsCancellationHandler.CancellationData> emptyList = MetroAtsCancellationHandler.filterDuplicates(new LinkedList<>());
        assertTrue(emptyList.isEmpty());
    }

    @Test
    public void testFilteringWithSingleCancellation() throws Exception {
        List<MetroAtsCancellationHandler.CancellationData> input = new LinkedList<>();
        input.add(mockCancellation(InternalMessages.TripCancellation.Status.CANCELED));
        List<MetroAtsCancellationHandler.CancellationData> result = MetroAtsCancellationHandler.filterDuplicates(input);
        assertEquals(1, result.size());
    }

    @Test
    public void testFilteringWithSingleRunning() throws Exception {
        List<MetroAtsCancellationHandler.CancellationData> input = new LinkedList<>();
        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING));
        List<MetroAtsCancellationHandler.CancellationData> result = MetroAtsCancellationHandler.filterDuplicates(input);
        assertEquals(1, result.size());
    }

    @Test
    public void testFilteringWithBothStatusesForSameDvjId() throws Exception {
        long dvjId = MockDataUtils.generateValidJoreId();
        List<MetroAtsCancellationHandler.CancellationData> input = new LinkedList<>();

        input.add(mockCancellation(InternalMessages.TripCancellation.Status.CANCELED, dvjId));
        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING, dvjId));
        List<MetroAtsCancellationHandler.CancellationData> result = MetroAtsCancellationHandler.filterDuplicates(input);
        assertEquals(1, result.size());
        assertEquals(InternalMessages.TripCancellation.Status.CANCELED, result.get(0).getPayload().getStatus());
    }

    @Test
    public void testFilteringWithMultipleRunningForSameDvjId() throws Exception {
        long dvjId = MockDataUtils.generateValidJoreId();
        List<MetroAtsCancellationHandler.CancellationData> input = new LinkedList<>();

        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING, dvjId));
        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING, dvjId));
        List<MetroAtsCancellationHandler.CancellationData> result = MetroAtsCancellationHandler.filterDuplicates(input);
        assertEquals(1, result.size());
        assertEquals(InternalMessages.TripCancellation.Status.RUNNING, result.get(0).getPayload().getStatus());
    }

    @Test
    public void testFilteringWithMultipleRunningForDifferentDvjId() throws Exception {
        long firstDvjId = MockDataUtils.generateValidJoreId();
        long secondDvjId = firstDvjId++;
        List<MetroAtsCancellationHandler.CancellationData> input = new LinkedList<>();

        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING, firstDvjId));
        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING, secondDvjId));
        List<MetroAtsCancellationHandler.CancellationData> result = MetroAtsCancellationHandler.filterDuplicates(input);
        assertEquals(2, result.size());
        assertEquals(0, result.stream().filter(data -> data.getPayload().getStatus() == InternalMessages.TripCancellation.Status.CANCELED).count());
        assertEquals(2, result.stream().filter(data -> data.getPayload().getStatus() == InternalMessages.TripCancellation.Status.RUNNING).count());
    }

    @Test
    public void testFilteringWithBothStatusesForDifferentDvjId() throws Exception {
        long firstDvjId = MockDataUtils.generateValidJoreId();
        long secondDvjId = firstDvjId++;
        List<MetroAtsCancellationHandler.CancellationData> input = new LinkedList<>();

        input.add(mockCancellation(InternalMessages.TripCancellation.Status.CANCELED, firstDvjId));
        input.add(mockCancellation(InternalMessages.TripCancellation.Status.RUNNING, secondDvjId));
        List<MetroAtsCancellationHandler.CancellationData> result = MetroAtsCancellationHandler.filterDuplicates(input);
        assertEquals(2, result.size());
        assertEquals(1, result.stream().filter(data -> data.getPayload().getStatus() == InternalMessages.TripCancellation.Status.CANCELED).count());
        assertEquals(1, result.stream().filter(data -> data.getPayload().getStatus() == InternalMessages.TripCancellation.Status.RUNNING).count());
    }


    private MetroAtsCancellationHandler.CancellationData mockCancellation(InternalMessages.TripCancellation.Status status) throws Exception {
        long dvjId = MockDataUtils.generateValidJoreId();
        return mockCancellation(status, dvjId);
    }

    private MetroAtsCancellationHandler.CancellationData mockCancellation(InternalMessages.TripCancellation.Status status, long dvjId) throws Exception {
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(dvjId,
                "7575",
                PubtransFactory.JORE_DIRECTION_ID_INBOUND,
                "20180101",
                "11:22:00",
                status);
        return new MetroAtsCancellationHandler.CancellationData(cancellation, System.currentTimeMillis(), Long.toString(dvjId));
    }
    */
}
