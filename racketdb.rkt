#lang racket

(require
  (rename-in "proto/ql2.rkt"
    [version-dummy:version->integer v->int]
    [term:term-type->integer term->int])
  (rename-in racket/base
    [not not-]
    [floor floor-]
    [round round-]
    [append append-] 
    [values values-]
    [map map-]
    [filter filter-]
    [or or-]
    [and and-]
    [for-each for-each-])
  json)

(provide
  run/123 serialize-reql-term
  connect close-connection datum array object db db-create db-drop db-list table get get-all eq ne
  lt le gt ge add sub mul div mod ceil prepend difference set-insert set-intersection set-union
  set-difference slice skip limit offsets-of contains get-field keys has-fields with-fields pluck
  without merge between reduce fold concat-map order-by distinct count is-empty union nth bracket
  inner-join outer-join eq-join zip range insert-at delete-at change-at splice-at coerce-to type-of
  update delete replace insert table-create table-drop table-list config wait reconfigure rebalance
  sync grant index-create index-drop index-list index-status index-wait index-rename set-write-hook
  get-write-hook funcall branch run run*
  (rename-out
    [not r:not]
    [floor r:floor] 
    [round r:round]
    [append r:append] 
    [values r:values]
    [map r:map]
    [filter r:filter]
    [or r:or]
    [and r:and]
    [for-each r:for-each])) 

;; *************************************************************************************************
;; *************************************************************************************************

(define (read-null-term-str in)
  (let ([c (read-char in)])
    (if (char=? c #\nul) '() (cons c (read-null-term-str in)))))

(define (connect [host "127.0.0.1"] [port 28015])
  (define-values [in out] (tcp-connect host port))
  (write-bytes (integer->integer-bytes (v->int 'v0-4) 4 #f) out)
  (write-bytes (bytes #x00 #x00 #x00 #x00) out)
  (write-bytes (integer->integer-bytes (version-dummy:protocol->integer 'json) 4 #f) out)
  (flush-output out)
  (let ([response (read-null-term-str in)])
    (if (string=? (list->string response) "SUCCESS") (list in out) '())))

(define (close-connection connection)
  (close-input-port (first connection))
  (close-output-port (second connection)))

;; *************************************************************************************************
;; *************************************************************************************************

(struct reql-term (type args optargs) #:transparent)

(define-syntax-rule (defn-op-anchored opname)
  (define-syntax-rule (opname arg (... ...))
    (lambda (builder)
      (builder (reql-term (term->int 'opname) (list arg (... ...)) '#hash())))))

(define-syntax-rule (defn-op-chained opname)
  (define-syntax (opname stx)
    (syntax-case stx (with)
      [(_ arg (... ...) with opt)
       #'(lambda (chained)
           (lambda (builder)
             (builder (reql-term (term->int 'opname) (list chained arg (... ...)) opt))))]
      [(_ arg (... ...))
       #'(lambda (chained)
           (lambda (builder)
             (builder (reql-term (term->int 'opname) (list chained arg (... ...)) '#hash()))))])))

(define-syntax-rule (define-anchored-ops op ...)
  (begin (defn-op-anchored op) ...))

(define-syntax-rule (define-chained-ops op ...)
  (begin (defn-op-chained op) ...))

;; *************************************************************************************************
;; *************************************************************************************************

(define-anchored-ops
  datum array object db db-create db-drop db-list)

(define-chained-ops
  table get get-all
  eq ne lt le gt ge not
  add sub mul div mod
  floor ceil round
  append prepend difference
  set-insert set-intersection set-union set-difference
  slice skip limit offsets-of contains
  get-field keys values has-fields with-fields pluck without merge
  between reduce map fold filter concat-map order-by distinct count is-empty union nth bracket
  inner-join outer-join eq-join zip range
  insert-at delete-at change-at splice-at
  coerce-to type-of
  update delete replace insert
  table-create table-drop table-list config wait reconfigure rebalance sync grant
  index-create index-drop index-list index-status index-wait index-rename
  set-write-hook get-write-hook
  funcall branch or and for-each)

;; *************************************************************************************************
;; *************************************************************************************************

(define (serialize-reql-term term)
  (define (serialize-datum val)
    (cond [(symbol? val) (if (eq? val 'null) (symbol->string val) ;; no quotes around null
                           (string-append "\"" (symbol->string val) "\""))]
          [(number? val) (number->string val)]
          [(hash? val) (string-append "{" (serialize-hash val) "}")]
          [else (string-append "\"" val "\"")]))
  (define (reql-datum? val)
    (or- (symbol? val) (number? val) (hash? val) (string? val)))
  (define (serialize-reql-term-list terms)
    (cond [(empty? terms) ""]
          [(empty? (rest terms)) (serialize-reql-term (first terms))]
          [else
            (string-append
              (serialize-reql-term (first terms))
              ","
              (serialize-reql-term-list (rest terms)))]))
  (define (serialize-hash h)
    (define tmp (hash-map h (lambda (k v) (string-append (serialize-datum k) ":" (serialize-datum v)))))
    (foldr (lambda (x y) (string-append x "," y)) (first tmp) (rest tmp)))
  (cond [(reql-datum? term) (serialize-datum term)]
        [else (string-append "["
                             (number->string (reql-term-type term))
                             (string-append ",[" (serialize-reql-term-list (reql-term-args term)) "]")
                             (if (zero? (hash-count (reql-term-optargs term))) ""
                                 (string-append ",{" (serialize-hash (reql-term-optargs term)) "}"))
                             "]")]))

;; *************************************************************************************************
;; *************************************************************************************************

(define TOKEN 0)
(define (get-token) (begin (set! TOKEN (remainder (add1 TOKEN) (expt 2 64))) TOKEN))

;; *************************************************************************************************
;; *************************************************************************************************

(define-struct (client-error exn:fail) ()) 
(define-struct (compile-error exn:fail) ()) 
(define-struct (runtime-error exn:fail) ()) 

(define (write-query token query out)
  (write-bytes (integer->integer-bytes token 8 #f) out)
  (write-bytes (integer->integer-bytes (string-length query) 4 #f) out)
  (write-bytes (string->bytes/utf-8 query) out)
  (flush-output out))

(define (start-query query token connection)
  (let* ([type (number->string (query:query-type->integer 'start))]
         [query-with-start (string-append "[" type "," query ",{}]")]
         [out (second connection)])
    (write-query token query-with-start out)))

(define (continue-query token connection)
  (let* ([type (number->string (query:query-type->integer 'continue))]
         [query (string-append "[" type "," token "]")]
         [out (second connection)])
    (write-query token query out)))

(define (stop-query token connection)
  (let* ([type (number->string (query:query-type->integer 'stop))]
         [query (string-append "[" type "]")]
         [out (second connection)])
    (write-query token query out)))

(define (noreply-wait token connection)
  (let* ([type (number->string (query:query-type->integer 'noreply-wait))]
         [query (string-append "[" type "]")]
         [out (second connection)])
    (write-query token query out)))

(define (read-response token connection)
  (define (complete-response->stream rsp)
    (cond [(empty? rsp) empty-stream]
          [else (stream-cons (first rsp) (complete-response->stream (rest rsp)))]))
  (define (partial-response->stream rsp)
    (cond [(empty? rsp)
           (continue-query token connection)
           (read-response token connection)]
          [else (stream-cons (first rsp) (partial-response->stream rsp))]))
  (let ([in (first connection)])
    ;; query token (8 bytes)
    (if (= token (integer-bytes->integer (read-bytes 8 in) #f))
        (begin
          ;; response size (4 bytes)
          (read-bytes 4 in)
          ;; hash from json-encoded rsponse
          (let* ([rsp (read-json in)]
                 [type (hash-ref rsp 't)]
                 [data (hash-ref rsp 'r)])
            (cond [(= type (response:response-type->integer 'success-atom)) data]
                  [(= type (response:response-type->integer 'success-sequence)) (complete-response->stream data)]
                  [(= type (response:response-type->integer 'success-partial)) (partial-response->stream data)]
                  [(= type (response:response-type->integer 'client-error))
                            (raise (client-error (first data) (current-continuation-marks)))]
                  [(= type (response:response-type->integer 'compile-error))
                            (raise (compile-error (first data) (current-continuation-marks)))]
                  [(= type (response:response-type->integer 'runtime-error))
                            (raise (runtime-error (first data) (current-continuation-marks)))])))
        (error "Query token does not match"))))

;; *************************************************************************************************
;; *************************************************************************************************

(define (run/123 . args)
  (cond ((empty? args) (lambda (x) x))
        ((empty? (rest args)) ((first args) (lambda (x) x)))
        (else
          (let ((chain ((first args) (second args))))
            (apply run/123 (cons chain (rest (rest args))))))))

(define (run/456 . args)
  (apply run/123 (cons (db "test") args)))

(define (run1 query connection)
  (let ([token (get-token)])
    (start-query (serialize-reql-term query) token connection)
    (read-response token connection)))

(define (run connection . query)
  (run1 (apply run/123 query) connection))

(define (run* connection . query)
  (run1 (apply run/456 query) connection))
