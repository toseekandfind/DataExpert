# NBA Analytics Incident Response Playbook

## Incident Classification Matrix

### Severity Levels
| Level | Description | Example | Response Time | Resolution Time |
|-------|-------------|---------|---------------|-----------------|
| P0 | Service Down | Live game feed outage | 15 min | 1 hour |
| P1 | Severe Impact | Delayed statistics | 30 min | 2 hours |
| P2 | Moderate Impact | Historical data issues | 2 hours | 8 hours |
| P3 | Minor Impact | UI formatting issues | 24 hours | 48 hours |

## Initial Response Procedures

### 1. Incident Detection
- Monitor alerts in PagerDuty
- Check Datadog dashboards
- Review error logs
- Assess system health metrics

### 2. First Response Actions
```bash
# 1. Check system status
systemctl status nba-analytics-*

# 2. Verify database connectivity
psql -h $DB_HOST -U $DB_USER -d nba_stats -c "SELECT 1;"

# 3. Check pipeline logs
tail -f /var/log/nba-analytics/pipeline.log

# 4. Monitor real-time metrics
curl http://metrics.internal/nba-analytics/health
```

### 3. Communication Protocol
1. Acknowledge incident in PagerDuty
2. Post initial status in #nba-analytics-incidents
3. Update status page
4. Notify stakeholders if P0/P1

## Common Issues and Solutions

### 1. Data Pipeline Failures
```sql
-- Check for incomplete game data
SELECT game_id, COUNT(*) as stat_count
FROM game_details
WHERE game_date = CURRENT_DATE
GROUP BY game_id
HAVING COUNT(*) < 240;  -- Expected stats per game

-- Verify data consistency
SELECT g.game_id, 
       g.pts_home, 
       SUM(gd.pts) as calculated_pts
FROM games g
JOIN game_details gd ON g.game_id = gd.game_id
WHERE g.game_date = CURRENT_DATE
GROUP BY g.game_id, g.pts_home
HAVING g.pts_home != SUM(gd.pts);
```

### 2. Real-time Processing Issues
```python
# Check processing lag
def check_processing_lag():
    current_time = datetime.now()
    last_processed = get_last_processed_timestamp()
    lag = (current_time - last_processed).seconds
    return lag > 300  # Alert if > 5 minutes behind
```

### 3. Database Performance
```sql
-- Check for blocking queries
SELECT blocked_locks.pid AS blocked_pid,
       blocking_locks.pid AS blocking_pid,
       blocked_activity.usename AS blocked_user,
       blocking_activity.usename AS blocking_user,
       now() - blocked_activity.xact_start AS blocked_duration
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype;
```

## Recovery Procedures

### 1. Data Recovery
```bash
# Restore from backup if needed
pg_restore -h $DB_HOST -U $DB_USER -d nba_stats backup/nba_stats_YYYY_MM_DD.dump

# Verify data integrity
psql -h $DB_HOST -U $DB_USER -d nba_stats -f scripts/validate_data.sql
```

### 2. Service Recovery
```bash
# Restart services in order
systemctl restart nba-analytics-ingestion
sleep 30
systemctl restart nba-analytics-processing
sleep 30
systemctl restart nba-analytics-api
```

### 3. Cache Recovery
```python
def rebuild_cache():
    redis_client.flushall()  # Clear existing cache
    rebuild_game_cache()
    rebuild_player_cache()
    rebuild_team_cache()
```

## Post-Incident Procedures

### 1. Incident Report Template
```markdown
# Incident Report: [ID]
Date: [DATE]
Duration: [DURATION]
Severity: [P0-P3]

## Timeline
- [TIME] Initial alert
- [TIME] Investigation started
- [TIME] Root cause identified
- [TIME] Resolution implemented
- [TIME] Service restored

## Root Cause
[DETAILED DESCRIPTION]

## Resolution
[STEPS TAKEN]

## Prevention
[FUTURE MITIGATIONS]
```

### 2. Metrics Collection
```sql
-- Capture incident metrics
INSERT INTO incident_metrics (
    incident_id,
    detection_time,
    response_time,
    resolution_time,
    severity,
    impact_score
) VALUES ($1, $2, $3, $4, $5, $6);
```

### 3. Improvement Tracking
```sql
-- Track action items
CREATE TABLE incident_actions (
    action_id SERIAL PRIMARY KEY,
    incident_id TEXT,
    action_description TEXT,
    assigned_to TEXT,
    due_date DATE,
    status TEXT,
    completion_date DATE
);
```

## Training Requirements

### 1. Required Training Modules
- Incident Response Basics
- NBA Data Systems Architecture
- Alert Management
- Communication Protocols
- Recovery Procedures

### 2. Certification Process
- Complete training modules
- Pass practical exercises
- Shadow senior responders
- Handle test incidents
- Regular recertification

## Documentation Standards

### 1. Incident Documentation
- Use standard templates
- Include all commands run
- Document timing of events
- Track communication
- Record decisions made

### 2. Runbook Updates
- Review after each incident
- Add new scenarios
- Update procedures
- Verify accuracy
- Get peer review 