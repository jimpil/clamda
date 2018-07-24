(ns clamda.redux
  (:import (java.util.stream Stream StreamSupport)
           (java.util Spliterator)))


(defn- abortive-spliterator ;; short-circuiting Spliterator
  "Wraps a Spliterator such that it can (optionally) terminate early
  (i.e. when <done?>, a fn of no args, returns true)."
  [^Spliterator internal done?]
  (reify Spliterator
    (characteristics [_]
      (-> internal
          .characteristics
          (bit-and-not Spliterator/SIZED Spliterator/SORTED)))
    (estimateSize [_]
      (if (done?)
        0
        (.estimateSize internal)))
    (tryAdvance [_ action]
      (and (not (done?))
           (.tryAdvance internal action)))
    (trySplit [_]
      (when-let [new-split (.trySplit internal)]
        (abortive-spliterator new-split done?)))))

(defn abortive-stream ;; ;; short-circuiting Stream
  "Wraps a Stream such that it can (optionally) terminate early
  (i.e. when <done?>, a fn of no args, returns true)."
  ^Stream [^Stream stream done?]
  (let [parallel? (.isParallel stream)]
    (-> stream
        .spliterator
        (abortive-spliterator done?)
        (StreamSupport/stream parallel?))))

(deftype SeqSpliterator
  [^:unsynchronized-mutable s rest-fn lazy-seq-split]

  Spliterator
  (characteristics [_]
    (cond->> Spliterator/IMMUTABLE
             (counted? s)  (bit-and Spliterator/SIZED)
             (sorted? s)   (bit-and Spliterator/SORTED)
             (or (map? s)
                 (set? s)) (bit-and Spliterator/DISTINCT)))
  (estimateSize [_]
    (if (counted? s)
      (count s)
      Long/MAX_VALUE))
  (tryAdvance [_ action]
    (boolean
      (when-let [x (first s)]
        (.accept action x)
        (set! s (rest-fn s x)))))
  (trySplit [_]
    (when-let [current (not-empty s)]
      (cond
        (vector? current)
        (let [n (count current)
              middle (long (/ n 2))
              left  (subvec current 0 middle)
              right (subvec current middle n)] ;; fast split
          (set! s right)
          (SeqSpliterator. left rest-fn nil))

        (or (map? current)
            (set? current))
        (let [n (count current)
              middle (long (/ n 2))
              [left right] (split-at middle current)] ;; slow split
          (set! s (into (empty current) right))
          (SeqSpliterator. (into (empty current) left) rest-fn nil))

        :else
        (let [[left right] (split-at lazy-seq-split current)] ;; slow split
          (when (seq right)
            (set! s right)
            (SeqSpliterator. left rest-fn lazy-seq-split)))
        )
      )
    )
  )

(defn vrest
  "rest-fn for vectors"
  [v _]
  (subvec v 1))

(defn mrest
  "rest-fn for maps"
  [m me]
  (dissoc m (key me)))

(defn rrest
  "rest-fn for anything else"
  [s _]
  (rest s))
