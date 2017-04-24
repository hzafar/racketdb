#lang racket

(require
  "../racketdb.rkt"
  rackunit
  rackunit/log)

(let ([c (connect)])
  (let ([result (run (datum 3) (eq 3) with c)])
    (check-true (first result)))

  (let ([result (run (datum 4) (eq 3) with c)])
    (check-false (first result)))

  (let ([result (run (datum 3) (ne 95) with c)])
    (check-true (first result)))

  (let ([result (run (datum "hello") (ne "hello") with c)])
    (check-false (first result)))

  (let ([result (run (datum 3) (le 3 4 5) with c)])
    (check-true (first result)))

  (let ([result (run (datum 3) (le 4 3 5) with c)])
    (check-false (first result)))

  (let ([result (run (datum 3) (lt 3) with c)])
    (check-false (first result)))

  (let ([result (run (datum 45) (ge 7) with c)])
    (check-true (first result)))

  (let ([result (run (datum 3) (eq 75) (r:not) with c)])
    (check-true (first result)))

  (close-connection c))


(test-log #:display? #t #:exit? #t)
