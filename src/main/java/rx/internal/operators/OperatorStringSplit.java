package rx.internal.operators;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import rx.Observable.Operator;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

public class OperatorStringSplit implements Operator<String, String> {
    private final Pattern pattern;

    public OperatorStringSplit(final Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public Subscriber<? super String> call(Subscriber<? super String> child) {
        StringSplitSubscriber cs = new StringSplitSubscriber(child, pattern);
        child.add(cs);
        StringSplitProducer cp = new StringSplitProducer(cs);
        child.setProducer(cp);
        return cs;
    }

    static final class StringSplitProducer implements Producer {
        private StringSplitSubscriber cs;

        public StringSplitProducer(StringSplitSubscriber cs) {
            this.cs = cs;
        }

        @Override
        public void request(long n) {
            cs.requestFromChild(n);
        }
    }

    static final class StringSplitSubscriber extends Subscriber<String> {
        private final NotificationLite<String> nl = NotificationLite.instance();

        private final Pattern pattern;
        private final ConcurrentLinkedQueue<Object> queue;

        private final AtomicLong requested = new AtomicLong();
        private final Subscriber<? super String> child;

        public StringSplitSubscriber(Subscriber<? super String> s, Pattern pattern) {
            super(s);
            this.pattern = pattern;
            this.queue = new ConcurrentLinkedQueue<Object>();
            this.child = s;

            add(Subscriptions.create(new Action0() {
                @Override
                public void call() {
                    queue.clear();
                }
            }));
        }

        private void requestFromChild(long n) {
            if (n <= 0)
                return;
            // we track 'requested' so we know whether we should subscribe the next or not

            long previous = BackpressureUtils.getAndAddRequest(requested, n);
            if (previous == 0) {
                request(2);
            }
        }

        private String leftOver = null;

        @Override
        public void onNext(String segment) {
            if (leftOver != null)
                segment = leftOver + segment;
            String[] parts = pattern.split(segment, -1);

            for (int i = 0; i < parts.length - 1; i++) {
                String part = parts[i];
                if (part.isEmpty())
                    emptyPartCount++;
                else
                    queue.add(nl.next(part));
            }
            String last = parts[parts.length - 1];
            leftOver = last;

            emitLoop();
        }

        @Override
        public void onError(Throwable e) {
            child.onError(e);
            unsubscribe();
        }

        @Override
        public void onCompleted() {
            if (leftOver != null)
                queue.add(nl.next(leftOver));
            queue.add(nl.completed());
            emitLoop();
        }

        /**
         * when limit == 0 trailing empty parts are not emitted. Lazily keep
         * track of the number of empty parts just in case the there is a
         * non-empty part before the stream ends.
         * @param part
         */
        private int emptyPartCount = 0;

        void emitLoop() {
            long currRequested = requested.get();
            for (;;) {
                long queueSize = queue.size();

                if (Math.min(queueSize, currRequested) == 0) {
                    if (currRequested + emptyPartCount > 0)
                        request(1);
                    return;
                }

                long send = Math.min(queueSize + emptyPartCount, currRequested);
                for (int i = 0; i < send; i++) {
                    // use peek because all available requests could be used sending empty strings
                    Object n = queue.peek();

                    if (nl.isCompleted(n)) {
                        queue.poll();
                        child.onCompleted();
                    } else if (nl.isNext(n)) {
                        String part = nl.getValue(n);
                        // dump all proceeding empty strings
                        for (; emptyPartCount > 0 && i < send; emptyPartCount--, i++) {
                            child.onNext("");
                        }
                        // are there any remaining request left to send the actual value
                        if (i < send) {
                            queue.poll();
                            child.onNext(part);
                        }
                    }
                }

                currRequested = BackpressureUtils.produced(requested, send);
            }
        }
    }
}
