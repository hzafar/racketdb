#lang racket

(require grommet/crypto/hmac
         rackunit
         rackunit/log)

(provide hmac-sha256 pbkdf2)

(define hlen 32) ; using hmac-sha256

(define (pbkdf2 prf password salt iter dklen)
  (define num-blocks (ceiling (/ dklen hlen)))
  (subbytes (foldr bytes-append #""
              (map (λ (i) (block prf password salt (sub1 iter) i))
                (build-list num-blocks add1)))
    0 dklen))

(define (block prf password salt c i)
  (define (iter c u acc)
    (define (xored bs1 bs2)
      (list->bytes (map (λ (x y) (bitwise-xor x y)) (bytes->list bs1) (bytes->list bs2))))
    (if (zero? c)
        (xored u acc)
        (let ([u- (prf password u)])
          (iter (sub1 c) u- (xored u acc)))))
  (iter c
    (prf password (bytes-append salt (integer->integer-bytes i 4 #t #t)))
    (make-bytes hlen)))
