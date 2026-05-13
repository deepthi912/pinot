/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert;

import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class UpsertUtilsTest {

  @Test
  public void testHasNoQueryableDocsLiveQueryableEmpty() {
    IndexSegment segment = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap queryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(queryable.isEmpty()).thenReturn(true);
    when(segment.getQueryableDocIds()).thenReturn(queryable);
    assertTrue(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsLiveQueryableNonEmpty() {
    IndexSegment segment = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap queryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(queryable.isEmpty()).thenReturn(false);
    when(segment.getQueryableDocIds()).thenReturn(queryable);
    assertFalse(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsFallsBackToValidDocIdsWhenQueryableMissing() {
    IndexSegment segment = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap valid = mock(ThreadSafeMutableRoaringBitmap.class);
    when(valid.isEmpty()).thenReturn(true);
    when(segment.getQueryableDocIds()).thenReturn(null);
    when(segment.getValidDocIds()).thenReturn(valid);
    assertTrue(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsReturnsFalseWhenBothBitmapsMissing() {
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getQueryableDocIds()).thenReturn(null);
    when(segment.getValidDocIds()).thenReturn(null);
    assertFalse(segment.hasNoQueryableDocs());
  }

  // -------- Consistency-mode segments: snapshot is the source of truth. --------

  @Test
  public void testHasNoQueryableDocsConsistencyModeSnapshotEmpty() {
    // Even if the live bitmap has docs, the snapshot's view (empty) is what the query will scan.
    IndexSegment segment = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap liveQueryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(liveQueryable.isEmpty()).thenReturn(false);
    when(segment.getQueryableDocIds()).thenReturn(liveQueryable);
    assertTrue(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsConsistencyModeSnapshotNonEmpty() {
    // Even if the live bitmap is empty, the snapshot has docs the query will scan.
    IndexSegment segment = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap liveQueryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(liveQueryable.isEmpty()).thenReturn(true);
    when(segment.getQueryableDocIds()).thenReturn(liveQueryable);
    MutableRoaringBitmap snapshot = new MutableRoaringBitmap();
    snapshot.add(0);
    assertFalse(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsConsistencyModeSnapshotAbsent() {
    // Consistency mode on, but this segment is not in the current refresh (first refresh hasn't
    // run, or segment was just tracked). Live bitmap might disagree with the upcoming snapshot,
    // so do not claim empty.
    IndexSegment segment = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap liveQueryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(liveQueryable.isEmpty()).thenReturn(true);
    when(segment.getQueryableDocIds()).thenReturn(liveQueryable);
    assertFalse(segment.hasNoQueryableDocs());
  }
}
