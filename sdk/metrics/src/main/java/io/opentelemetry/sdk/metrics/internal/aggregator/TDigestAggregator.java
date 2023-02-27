package io.opentelemetry.sdk.metrics.internal.aggregator;

import com.tdunning.math.stats.TDigest;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.descriptor.MetricDescriptor;
import io.opentelemetry.sdk.metrics.internal.exemplar.ExemplarReservoir;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public final class TDigestAggregator implements Aggregator<HistogramPointData, DoubleExemplarData> {
  private final double compression;

  private final Supplier<ExemplarReservoir<DoubleExemplarData>> reservoirSupplier;

  public TDigestAggregator(
      double compression, Supplier<ExemplarReservoir<DoubleExemplarData>> reservoirSupplier) {
    this.compression = compression;
    this.reservoirSupplier = reservoirSupplier;
  }

  @Override
  public AggregatorHandle<HistogramPointData, DoubleExemplarData> createHandle() {
    return new Handle(compression, reservoirSupplier.get());
  }

  @Override
  public MetricData toMetricData(
      Resource resource,
      InstrumentationScopeInfo instrumentationScopeInfo,
      MetricDescriptor metricDescriptor,
      Collection<HistogramPointData> points,
      AggregationTemporality temporality) {
    return ImmutableMetricData.createDoubleHistogram(
        resource,
        instrumentationScopeInfo,
        metricDescriptor.getName(),
        metricDescriptor.getDescription(),
        metricDescriptor.getSourceInstrument().getUnit(),
        ImmutableHistogramData.create(temporality, points));
  }

  static final class Handle extends AggregatorHandle<HistogramPointData, DoubleExemplarData> {

    private double sum;

    private TDigest tdigest;

    Handle(double compression, ExemplarReservoir<DoubleExemplarData> exemplarReservoir) {
      super(exemplarReservoir);
      this.tdigest = TDigest.createDigest(compression);
    }

    /**
     * Maps from TDigest Centroids to OpenTelemetry HistogramPointData.
     *
     * <p>There is a conceptual problem here: a centroid mean is not a boundary.
     *
     * <p>The list of counts is one plus the amount of centroids to satisfy expectations.
     * To reconstruct this TDigest, the last element of {@code HistogramPointData.counts} must be ignored.
     */
    @Override
    protected HistogramPointData doAggregateThenMaybeReset(
        long startEpochNanos,
        long epochNanos,
        Attributes attributes,
        List<DoubleExemplarData> exemplars,
        boolean reset) {
      tdigest.compress();
      final long samples = tdigest.size();
      final int centroids = tdigest.centroidCount();
      final List<Double> boundaries = new ArrayList<>(centroids);
      final List<Long> counts = new ArrayList<>(centroids + 1);
      tdigest
          .centroids()
          .forEach(
              it -> {
                boundaries.add(it.mean());
                counts.add((long) it.count());
              });
      if (reset) {
        sum = 0.0;
        tdigest = TDigest.createDigest(tdigest.compression());
      }
      return ImmutableHistogramPointData.create(
          startEpochNanos,
          epochNanos,
          attributes,
          sum,
          samples > 0,
          tdigest.getMin(),
          samples > 0,
          tdigest.getMax(),
          boundaries,
          counts,
          exemplars);
    }

    @Override
    protected void doRecordLong(long value) {
      sum += value;
      tdigest.add(value);
    }

    @Override
    protected void doRecordDouble(double value) {
      sum += value;
      tdigest.add(value);
    }
  }
}
