# 운영 전환 체크리스트

> 이 저장소는 **로컬 학습/검증** 목적으로, 의도적으로 단순화한 타협이 곳곳에 있다.
> 운영 환경으로 나갈 때 **반드시 변경·수정해야 할 항목**을 계층별로 정리한다.
> 각 항목: 현재 상태(로컬 타협) → 운영 요구. ★ = 보안상 미변경 시 즉시 위험.

---

## 1. 통신·암호화

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ Kafka 리스너 | 전 구간 PLAINTEXT | SASL_SSL (인증 §2와 함께) — 브로커간·클라이언트간 TLS |
| ★ MinIO(S3) | `s3_use_ssl=false`, http | TLS 종단 + 인증서 관리(사설 CA 또는 관리형 S3 전환) |
| ★ 내부 HTTP 전부 | Nessie·Connect·Apicurio·Airflow 모두 http | TLS 또는 서비스메시(mTLS). 특히 Connect REST(커넥터 설정에 DB 비밀번호 포함)와 Airflow UI 우선 |
| 포트 노출 | `127.0.0.1` 바인딩(단일 호스트 전제) | 다중 호스트에선 무의미 — 네트워크 정책/보안그룹/방화벽으로 재설계 |

## 2. 인증·인가

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ Kafka | 인증 없음 (누구나 produce/consume — 포이즌 주입 테스트가 가능했던 이유이기도 함) | SASL(SCRAM/mTLS/OAUTHBEARER) + **토픽별 ACL** (프로듀서=Debezium만, 컨슈머 그룹 분리) |
| ★ Kafka Connect REST | 무인증 (커넥터 등록/삭제/오프셋 삭제 전부 열림) | 인증 프록시 또는 basic auth + 네트워크 격리. 오프셋 DELETE 같은 파괴적 API는 특히 |
| ★ Apicurio 레지스트리 | 무인증 (아무나 스키마 등록/삭제 가능) | 인증 + 역할 분리(읽기=전원, 등록=CI만). auto-register 끄고 CI 파이프라인에서 스키마 등록 |
| ★ MinIO | root 계정 하나로 전부 | IAM 사용자·버킷 정책 분리(쓰기=컨슈머·Spark, 읽기=배치), root는 봉인 |
| ★ Grafana | 익명 Admin (`GF_AUTH_ANONYMOUS_ORG_ROLE: Admin`) | 익명 접속 제거, SSO/OAuth + Viewer/Editor 역할 분리 |
| Airflow | standalone 자동생성 계정 | RBAC + SSO, 배포용 서비스계정 분리 |
| Postgres | 단일 슈퍼계정(settle)을 앱·CDC·Airflow가 공유 | 역할 분리: Debezium=REPLICATION 전용, Airflow=airflowdb 전용, 앱=최소 권한. `pg_hba` 제한 |

## 3. 자격증명·시크릿

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ 보관 위치 | `infra/.env` 평문 파일 (gitignore로만 보호) | 시크릿 매니저(Vault·AWS Secrets Manager 등) + 주입 시점 분리. 파일로 떨어뜨리지 않기 |
| ★ 약한 값 | `settle/settle`, `admin/admin` 류 개발용 값 | 생성형 강 비밀번호 + 정기 로테이션. **기존 pg-data 볼륨이 초기 비밀번호에 묶여 있으므로 마이그레이션 계획 필요** |
| ★ Spark 자격증명 | `spark-sql.sh`가 `--conf`로 키 주입 → **`ps`에 노출** (알려진 한계) | Nessie credential-vending 또는 인스턴스 프로파일/IRSA로 전환 — CLI 인자에서 제거 |
| Debezium 커넥터 | 등록 시 평문 password가 `_connect_configs` 토픽에 저장됨 | Connect `config.providers`(파일/Vault 프로바이더)로 참조 치환 |

## 4. 가용성·데이터 내구성

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ Kafka | 브로커 1대, RF=1, `min.insync.replicas` 미설정 | 브로커 3+ · RF≥3 · min.isr=2 · 컨트롤러 분리(KRaft 3+) |
| ★ Postgres | 단일 인스턴스, 백업 없음 | PITR 백업(WAL 아카이브) + 복제본. **replication slot 감시**(컨슈머 다운 시 WAL 무한 적체 → 디스크 풀) |
| MinIO | 단일 노드 단일 드라이브 | 분산 모드(EC) 또는 관리형 S3 |
| Nessie | 인메모리 아님(카탈로그) but 단일 | HA 구성 + 백업. 카탈로그 유실 = Iceberg 테이블 접근 불능 |
| Airflow | standalone(스케줄러+웹서버 단일 프로세스), LocalExecutor | 스케줄러 HA + CeleryExecutor/KubernetesExecutor, 메타DB 백업 |
| retention | Kafka 기본 7일 | 최악 다운타임 + 재처리 여유보다 길게. DLT는 더 길게(포렌식) |
| 랜딩 NDJSON | 호스트 로컬 디스크 (`data/landing/`) | 랜딩 자체를 오브젝트 스토리지 직행으로 — 로컬 디스크는 SPOF·용량 무한 증가(append-only, 컴팩션 없음) |

## 5. 컨테이너·공급망

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ Airflow의 `docker.sock` 마운트 + root 실행 | 호스트 도커 제어권 공유 = **컨테이너 탈출 시 호스트 장악** (로컬 타협으로 명시함) | DockerOperator 제거 → KubernetesPodOperator/전용 러너. sock 마운트 금지, non-root |
| 이미지 태그 | 버전 태그 고정(A03 대응 완료) | **digest(@sha256) 고정** + 이미지 스캐너(Trivy 등) CI 게이트 + 사설 레지스트리 미러 |
| Gradle | `gradle.lockfile`만 | `verification-metadata.xml`(체크섬·서명 검증)까지. CI에서 `--offline` 빌드 |
| 빌드 다운로드 | DuckDB zip sha256 검증(완료) | 유지 + 사내 아티팩트 저장소 경유 |
| dbt | `dbt-duckdb==1.9.6` 고정(완료) | + `packages.yml` 잠금, 파이썬 베이스 이미지 digest 고정 |

## 6. 스키마 거버넌스 (Phase 8 실측 반영)

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ 호환성 규칙 | **실제 CDC 아티팩트에 미적용** — Apicurio 2.4.1은 참조(Source) 포함 아티팩트에 규칙 설정 시 기존 버전 파싱 실패(422, 실측) | 레지스트리 업그레이드(2.5+/3.x 검증) 후 전 아티팩트에 BACKWARD(또는 FULL) 규칙 적용. 그 전까지 비호환 변경 차단 장치가 없음 |
| auto-register | 컨버터가 신규 스키마 자동 등록 | 끄기 — 스키마 변경은 리뷰 거친 CI가 등록, 커넥터는 조회만 |
| key 직렬화 | JSON 유지(값만 Avro) | key도 Avro/스키마 관리로 통일 검토 (파티셔닝 키 계약 명시) |

## 7. 운영성·관측

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| ★ 알람 | 대시보드만 있고 **알람 없음** | 최소 세트: DLQ 유입(`settlement.dlq.records` > 0), 컨슈머 LAG 임계, replication slot 지연, 대사 diff≠0, Airflow 태스크 실패 |
| 대사 실패 처리 | checks가 출력만 함 (diff=0 눈으로 확인) | diff≠0 시 **배치 실패 + 알람 + 지급 중단** 게이트로 승격 |
| 로그 | 컨테이너 stdout, 수집 없음 | 중앙 수집(Loki/ELK) + 보존 정책. **민감정보 로깅 금지 정책 유지** |
| DLT 처리 | 격리까지만 (재처리 절차 없음) | DLT 소비·수동 재처리 runbook + 격리 건 알람 |
| 랜딩 컴팩션 | append-only 무한 증가 | 주기 컴팩션(dedup 반영분 재작성) 또는 Iceberg 직행 후 스냅샷 expire |
| 배치 멱등 재실행 | 검증됨(설계 강점 — 유지) | Airflow 재시도·백필과 결합해 runbook화 |

## 8. 정산 도메인 특화

| 항목 | 현재 (로컬) | 운영 요구 |
|---|---|---|
| 지급(payout) 실행 | 미구현 (정산 계산·대사까지) | 지급은 **대사 통과를 전제 조건**으로 하는 별도 승인 워크플로 + 감사 로그 |
| freeze 불변성 | TRUNCATE+재생성 멱등 | 마감 확정 후 원장 불변 보장(재실행 차단 or 버전 스탬프) + 정정은 조정 이벤트로만 |
| 시간대 | 로컬 단일 TZ 가정 | 정산일 경계 TZ 명시(KST 마감 등), Airflow `schedule`과 일치시킬 것 |
| 개인정보 | 합성 데이터 | 실데이터 유입 시 PII 분류·마스킹·보존 정책, 접근 감사 |

---

## 우선순위 제안 (운영 전환 시 착수 순서)

1. **§2 인증 + §3 시크릿** — 현재는 네트워크에 닿으면 전부 열림. 다른 모든 통제의 전제.
2. **§4 Kafka RF·Postgres 백업·slot 감시** — 데이터 유실은 되돌릴 수 없음.
3. **§7 알람(특히 대사 diff 게이트)** — 정산 도메인에서 조용한 오류가 최악.
4. **§1 TLS**, **§6 스키마 규칙**, **§5 digest 고정** — 순차 적용.
5. §7 나머지 운영성, §8 도메인 통제.

> 작성: 2026-09-01, Phase 9(Airflow)까지의 구현·실측 기준. 각 "현재" 칸은 STATUS.md의 해당 단계 기록에 근거.
