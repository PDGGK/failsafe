/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package dev.failsafe.functional;

import dev.failsafe.CircuitBreaker;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.event.ExecutionCompletedEvent;
import dev.failsafe.spi.Scheduler;
import dev.failsafe.testing.Testing;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Tests the configuration of an Executor.
 */
@Test
public class ExecutorConfigurationTest extends Testing {
  AtomicBoolean executorCalled;
  AtomicBoolean executionCalled;
  RetryPolicy<String> retryPolicy = withLogs(RetryPolicy.<String>builder()).build();

  Executor executor = execution -> {
    executorCalled.set(true);
    execution.run();
  };

  Executor throwingExecutor = execution -> {
    executorCalled.set(true);
    execution.run();
    throw new IllegalStateException();
  };

  @BeforeMethod
  protected void beforeMethod() {
    executorCalled = new AtomicBoolean();
    executionCalled = new AtomicBoolean();
  }

  public void testSyncExecutionSuccess() {
    String result = Failsafe.with(retryPolicy).with(executor).get(() -> {
      executionCalled.set(true);
      return "result";
    });

    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
    assertNull(result);
  }

  public void testSyncExecutionFailure() {
    assertThrows(() -> Failsafe.with(retryPolicy).with(executor).run(() -> {
      executionCalled.set(true);
      throw new IllegalStateException();
    }), IllegalStateException.class);

    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
  }

  public void testSyncExecutionThatThrowsFromTheExecutor() {
    assertThrows(() -> Failsafe.with(retryPolicy).with(throwingExecutor).run(() -> {
      executionCalled.set(true);
    }), IllegalStateException.class);

    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
  }

  public void testAsyncExecutionSuccess() throws Throwable {
    AtomicBoolean fjpAssertion = new AtomicBoolean();

    String result = Failsafe.with(retryPolicy).with(executor).getAsync(() -> {
      fjpAssertion.set(Thread.currentThread() instanceof ForkJoinWorkerThread);
      executionCalled.set(true);
      return "result";
    }).get();

    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
    assertTrue(fjpAssertion.get(), "the execution should run on a fork join pool thread");
    assertNull(result);
  }

  public void testAsyncExecutionFailure() {
    AtomicBoolean fjpAssertion = new AtomicBoolean();

    assertThrows(() -> Failsafe.with(retryPolicy).with(executor).getAsync(() -> {
      fjpAssertion.set(Thread.currentThread() instanceof ForkJoinWorkerThread);
      executionCalled.set(true);
      throw new IllegalStateException();
    }).get(), ExecutionException.class, IllegalStateException.class);

    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
    assertTrue(fjpAssertion.get(), "the execution should run on a fork join pool thread");
  }

  public void testAsyncExecutionThatThrowsFromTheExecutor() {
    assertThrows(() -> Failsafe.with(retryPolicy).with(throwingExecutor).runAsync(() -> {
      executionCalled.set(true);
    }).get(), ExecutionException.class, IllegalStateException.class);

    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
  }

  public void testComposeRetainsExecutor() throws Exception {
    FailsafeExecutor<String> composed = Failsafe.with(retryPolicy).with(executor)
      .compose(CircuitBreaker.ofDefaults());

    assertNull(composed.get(() -> {
      executionCalled.set(true);
      return "result";
    }));
    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());

    executorCalled.set(false);
    executionCalled.set(false);
    assertNull(composed.getAsync(() -> {
      executionCalled.set(true);
      return "result";
    }).get(5, TimeUnit.SECONDS));
    assertTrue(executorCalled.get());
    assertTrue(executionCalled.get());
  }

  @DataProvider
  public Object[][] executorServiceTypes() {
    return new Object[][] { { "executorService" }, { "scheduledExecutorService" }, { "executor" } };
  }

  @Test(dataProvider = "executorServiceTypes")
  public void testComposeRetainsExecutorService(String type) throws Exception {
    ExecutorService service = type.equals("scheduledExecutorService") ?
      Executors.newSingleThreadScheduledExecutor() : Executors.newSingleThreadExecutor();
    try {
      Thread expectedThread = service.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
      FailsafeExecutor<Thread> original = Failsafe.with(RetryPolicy.<Thread>ofDefaults());
      if (type.equals("scheduledExecutorService"))
        original.with((ScheduledExecutorService) service);
      else if (type.equals("executor"))
        original.with((Executor) service);
      else
        original.with(service);

      FailsafeExecutor<Thread> composed = original.compose(CircuitBreaker.ofDefaults());
      assertSame(composed.getAsync(Thread::currentThread).get(5, TimeUnit.SECONDS), expectedThread);
    } finally {
      service.shutdownNow();
    }
  }

  public void testComposeRetainsScheduler() throws Exception {
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    AtomicInteger scheduledCalls = new AtomicInteger();
    Scheduler scheduler = (callable, delay, unit) -> {
      scheduledCalls.incrementAndGet();
      return service.schedule(callable, delay, unit);
    };
    try {
      AtomicInteger attempts = new AtomicInteger();
      RetryPolicy<String> retry = RetryPolicy.<String>builder().withMaxRetries(1)
        .withDelay(Duration.ofMillis(1)).build();
      FailsafeExecutor<String> composed = Failsafe.with(retry).with(scheduler)
        .compose(CircuitBreaker.<String>builder().withFailureThreshold(2).build())
        .compose(RetryPolicy.<String>builder().withMaxRetries(0).build());

      assertEquals(composed.getAsync(() -> {
        if (attempts.getAndIncrement() == 0)
          throw new IllegalStateException();
        return "result";
      }).get(5, TimeUnit.SECONDS), "result");
      assertEquals(attempts.get(), 2);
      assertTrue(scheduledCalls.get() >= 2, "the configured scheduler should run the initial attempt and retry");
    } finally {
      service.shutdownNow();
    }
  }

  public void testComposeRetainsListeners() throws Exception {
    AtomicInteger successes = new AtomicInteger();
    AtomicInteger failures = new AtomicInteger();
    AtomicInteger completed = new AtomicInteger();
    CountDownLatch completions = new CountDownLatch(4);
    AtomicReference<ExecutionCompletedEvent<String>> success = new AtomicReference<>();
    AtomicReference<ExecutionCompletedEvent<String>> failure = new AtomicReference<>();
    FailsafeExecutor<String> composed = Failsafe.<String>none()
      .onSuccess(e -> {
        successes.incrementAndGet();
        success.set(e);
      })
      .onFailure(e -> {
        failures.incrementAndGet();
        failure.set(e);
      })
      .onComplete(e -> {
        completed.incrementAndGet();
        completions.countDown();
      })
      .compose(RetryPolicy.<String>builder().withMaxRetries(0).build());
    IllegalStateException exception = new IllegalStateException("expected");

    assertEquals(composed.get(() -> "result"), "result");
    assertThrows(() -> composed.run(() -> {
      throw exception;
    }), IllegalStateException.class);
    assertEquals(composed.getAsync(() -> "result").get(5, TimeUnit.SECONDS), "result");
    assertThrows(() -> composed.runAsync(() -> {
      throw exception;
    }).get(5, TimeUnit.SECONDS), ExecutionException.class, IllegalStateException.class);

    assertTrue(completions.await(5, TimeUnit.SECONDS), "all executions should notify the completion listener");
    assertEquals(completed.get(), 4);
    assertEquals(successes.get(), 2);
    assertEquals(failures.get(), 2);
    assertEquals(success.get().getResult(), "result");
    assertNull(success.get().getException());
    assertNull(failure.get().getResult());
    assertSame(failure.get().getException(), exception);
  }

  public void testComposeKeepsListenerConfigurationIndependent() {
    AtomicInteger inheritedCalls = new AtomicInteger();
    AtomicInteger originalCalls = new AtomicInteger();
    AtomicInteger composedCalls = new AtomicInteger();
    FailsafeExecutor<String> original = Failsafe.with(retryPolicy)
      .onSuccess(e -> inheritedCalls.incrementAndGet());
    FailsafeExecutor<String> composed = original.compose(CircuitBreaker.ofDefaults());

    original.onSuccess(e -> originalCalls.incrementAndGet());
    composed.get(() -> "result");
    assertEquals(inheritedCalls.get(), 1);
    assertEquals(originalCalls.get(), 0);

    composed.onSuccess(e -> composedCalls.incrementAndGet());
    original.get(() -> "result");
    composed.get(() -> "result");
    assertEquals(inheritedCalls.get(), 1);
    assertEquals(originalCalls.get(), 1);
    assertEquals(composedCalls.get(), 1);
  }

  public void testComposeKeepsPoliciesIndependentAndOrdered() {
    CircuitBreaker<String> breaker = CircuitBreaker.ofDefaults();
    RetryPolicy<String> inner = RetryPolicy.ofDefaults();
    FailsafeExecutor<String> original = Failsafe.with(retryPolicy);
    FailsafeExecutor<String> composed = original.compose(breaker);
    FailsafeExecutor<String> chained = composed.compose(inner);

    assertNotSame(composed, original);
    assertNotSame(chained, composed);
    assertEquals(original.getPolicies(), Collections.singletonList(retryPolicy));
    assertEquals(composed.getPolicies(), Arrays.asList(retryPolicy, breaker));
    assertEquals(chained.getPolicies(), Arrays.asList(retryPolicy, breaker, inner));
    assertEquals(chained.get(() -> "result"), "result");
    assertThrows(() -> original.compose(null), NullPointerException.class);
    assertEquals(original.getPolicies(), Collections.singletonList(retryPolicy));
  }
}
