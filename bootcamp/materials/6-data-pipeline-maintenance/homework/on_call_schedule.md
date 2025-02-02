# NBA Analytics Pipeline On-Call Schedule

## Primary On-Call Schedule (Q1 2024)

### Regular Weekly Rotation
| Week | Dates | Primary | Secondary | Notes |
|------|-------|---------|-----------|--------|
| W1 | Jan 1-7 | Alice Chen | Bob Smith | New Year's Day: David Wang on-call |
| W2 | Jan 8-14 | Charlie Davis | David Wang | |
| W3 | Jan 15-21 | Bob Smith | Alice Chen | MLK Day: Charlie Davis on-call |
| W4 | Jan 22-28 | David Wang | Charlie Davis | |
| W5 | Jan 29-Feb 4 | Emily Foster | Bob Smith | |
| W6 | Feb 5-11 | Charlie Davis | Emily Foster | |
| W7 | Feb 12-18 | Bob Smith | David Wang | President's Day: Emily Foster on-call |
| W8 | Feb 19-25 | Alice Chen | Charlie Davis | |

### Individual On-Call Load Distribution
| Engineer | Primary Weeks | Secondary Weeks | Total Weeks |
|----------|---------------|-----------------|-------------|
| Alice Chen | 2 | 1 | 3 |
| Bob Smith | 2 | 2 | 4 |
| Charlie Davis | 2 | 2 | 4 |
| David Wang | 1 | 2 | 3 |
| Emily Foster | 1 | 1 | 2 |

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

## Holiday Coverage Policy

### Major Holidays
- New Year's Day
- Martin Luther King Jr. Day
- President's Day
- Memorial Day
- Independence Day
- Labor Day
- Thanksgiving Day
- Christmas Day

### Holiday Coverage Rules
1. Holiday Premium Pay: 2x regular on-call rate
2. Maximum one holiday per engineer per quarter
3. Holiday shifts are 24 hours (midnight to midnight)
4. Engineers can volunteer for holiday coverage
5. Holiday coverage does not count toward regular rotation limits

## Shift Handoff Process

### Daily Handoff Meeting (9:00 AM EST)
1. Review active incidents
2. Discuss potential issues
3. Update runbooks if needed
4. Transfer monitoring responsibilities
5. Verify alert settings

### Weekly Handoff (Monday 9:00 AM EST)
1. Review week's incidents
2. Update documentation
3. Discuss system improvements
4. Plan for upcoming maintenance
5. Verify contact information

## Backup Coverage

### Emergency Backup Rotation
| Week | Primary Backup | Secondary Backup |
|------|---------------|------------------|
| W1-W2 | Emily Foster | David Wang |
| W3-W4 | Alice Chen | Charlie Davis |
| W5-W6 | Bob Smith | Emily Foster |
| W7-W8 | David Wang | Bob Smith |

### Backup Activation Process
1. Primary on-call unresponsive for 15 minutes
2. Secondary on-call unresponsive for 15 minutes
3. PagerDuty escalates to backup
4. Incident report required for backup activation

## Work-Life Balance Considerations

### Time-Off Management
1. Block out vacation time 4 weeks in advance
2. No on-call week before/after vacation
3. Compensation time for excessive after-hours work
4. Flexible shift trading allowed with 48-hour notice
5. Remote work support during on-call
6. Recovery day offered after high-severity incidents

### Health and Wellness
1. Minimum 8-hour rest period between incidents
2. Mental health support available
3. Stress management resources
4. Regular workload reviews
5. Quarterly wellness check-ins

## Training and Support

### Required Certifications
1. Pipeline Architecture Training
2. Incident Response Training
3. System Architecture Overview
4. AWS Operations Training
5. Database Recovery Training
6. Alert Management Certification
7. Communication Protocol Training
8. Tool Proficiency Verification

### Ongoing Education
1. Monthly technical workshops
2. Quarterly incident review sessions
3. Tool update training
4. Process improvement discussions
5. Industry best practices reviews
6. Quarterly refresher sessions

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
