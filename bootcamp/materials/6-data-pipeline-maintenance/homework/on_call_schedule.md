# NBA Analytics Pipeline On-Call Schedule

## Rotation Schedule
The on-call rotation follows a weekly schedule, starting Monday 9:00 AM EST and ending the following Monday 9:00 AM EST.

### Primary On-Call Schedule (Q1 2024)
| Week | Dates | Primary | Secondary | 
|------|-------|---------|-----------|
| W1 | Jan 1-7 | Alice Chen | Bob Smith |
| W2 | Jan 8-14 | Charlie Davis | Alice Chen |
| W3 | Jan 15-21 | Bob Smith | Charlie Davis |
| W4 | Jan 8-14 | Alice Chen | Bob Smith |
[...continues throughout quarter]

## On-Call Responsibilities

### Primary On-Call
- First responder to all alerts
- Must acknowledge alerts within 15 minutes
- Must begin investigation within 30 minutes
- Responsible for escalation decisions
- Must document all incidents in the incident management system

### Secondary On-Call
- Backup for primary on-call
- Takes over if primary is unavailable
- Provides support for complex incidents
- Reviews incident reports from previous shift

## Response Time Requirements
- P0 (Critical): 15 minutes
- P1 (High): 30 minutes
- P2 (Medium): 2 hours
- P3 (Low): Next business day

## Handoff Process
1. End of shift review meeting (30 mins)
2. Review open incidents and ongoing issues
3. Document status in handoff document
4. Verify alerting setup for next shift

## Escalation Path
1. Primary On-Call
2. Secondary On-Call
3. Team Lead
4. Engineering Manager
5. Director of Engineering

## Communication Channels
- Primary: PagerDuty
- Secondary: Slack (#nba-analytics-oncall)
- Emergency: Phone call cascade

## Tools and Access
Required access for on-call engineers:
- AWS Console (Admin)
- Datadog
- PagerDuty
- Airflow
- NBA Stats API
- Production Database Read/Write
- Incident Management System

## Training Requirements
All on-call engineers must complete:
1. Pipeline Architecture Training
2. Incident Response Workshop
3. AWS Operations Training
4. Database Recovery Training
5. Quarterly Refresher Sessions

## Compensation
- Weekly on-call stipend: $500
- Incident response time (outside business hours): 1.5x hourly rate
- Minimum 4 hours pay for any off-hours response

## Work-Life Balance Policies
1. Maximum 2 weeks of primary on-call per quarter
2. Minimum 2 weeks between on-call rotations
3. Swap shifts allowed with 48-hour notice
4. Recovery day offered after high-severity incidents

## Documentation Requirements
Each on-call shift must maintain:
1. Incident logs
2. Resolution steps taken
3. System improvements identified
4. Handoff notes
5. Updated runbooks (if applicable)

## Quarterly Review
The on-call program is reviewed quarterly for:
1. Alert frequency and validity
2. Response time metrics
3. Engineer workload balance
4. Process improvements
5. Training needs 