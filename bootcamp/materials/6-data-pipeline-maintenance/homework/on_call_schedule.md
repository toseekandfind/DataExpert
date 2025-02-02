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

### Rotation Fairness Guidelines
1. Maximum 2 primary weeks per quarter per engineer
2. Minimum 2 weeks between on-call shifts
3. Equal distribution of weekend coverage
4. Holiday rotation separate from regular schedule
5. Flexibility for shift trades with 48-hour notice

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
4. Flexible shift trading allowed
5. Remote work support during on-call

### Health and Wellness
1. Minimum 8-hour rest period between incidents
2. Mental health support available
3. Stress management resources
4. Regular workload reviews
5. Quarterly wellness check-ins

## Training and Support

### Required Certifications
1. Incident Response Training
2. System Architecture Overview
3. Alert Management Certification
4. Communication Protocol Training
5. Tool Proficiency Verification

### Ongoing Education
1. Monthly technical workshops
2. Quarterly incident review sessions
3. Tool update training
4. Process improvement discussions
5. Industry best practices reviews 