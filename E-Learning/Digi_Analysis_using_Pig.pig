--To create a relation named Participant_rel by defining the entire schema with appropriate datatypes 

Participant_rel = LOAD '/user/Digi_Analysis/Participants.csv' USING PigStorage(',') AS (
ParticipantId:int,
ParticipantMailId:chararray,
ParticipantCountry:chararray,
InstructorId:int,
CourseCode:chararray,
CourseOfferingId:int,
Source:chararray,
DateActionPerformed:chararray,
TrainingHours:int,
Location:chararray,
OfferingMode:chararray,
Status:chararray,
TrackName:chararray
);


--Create a relation named Courses_rel with defining the schema 

Courses_rel = LOAD '/user/Digi_Analysis/Courses.csv' USING PigStorage(',') AS (
CourseOwnerId:int,
CourseOwnerMailId:chararray,
CourseCode:chararray,
DepartmentCode:chararray,
Source:chararray,
DateCreated:chararray,
OfferingMode:chararray,
Stacks:chararray,
Status:chararray,
CourseTitle:chararray,
TaskName:chararray,
CourseDuration:int
);

--Create a relation named Courses_rel without schema

Courses_rel_without_schema = LOAD '/user/Digi_Analysis/Courses.csv' USING PigStorage(',');

--To verify the schema of the relations created

DESCRIBE Courses_rel;

DESCRIBE Courses_rel_without_schema;

--Display the contents of the relations created on the console

dump Participant_rel;

dump Courses_rel;

dump  Courses_rel_without_schema;

--Identify the most popular courses 

course_group = GROUP Courses_rel BY CourseTitle;

course_count= FOREACH course_group  GENERATE group AS CourseTitle ,COUNT(Courses_rel) AS popularity;

course_order = ORDER course_count BY popularity DESC;

--course_limit = LIMIT course_order 1;

STORE course_order INTO '/user/pig_output/popular_courses.csv' USING PigStorage(',');

--dump course_limit;


--Find the distinct countries from where C-Learn is been accessed 

colearn_data = FOREACH Participant_rel GENERATE ParticipantCountry,Location;

colearn_filter = FILTER colearn_data BY Location MATCHES 'Co-Learn_Application';

colearn_distinct = DISTINCT colearn_filter;

STORE colearn_distinct INTO '/user/pig_output/distinct_countries.csv' USING PigStorage(',');

--dump colearn_distinct;

--Filter course details based on various predicates -- Status,TrackName,CourseDuration,OfferingMode

course_status= FILTER Courses_rel BY Status == 'active';

course_SAP= FILTER Participant_rel BY TrackName == 'SAP';

course_duration = FILTER Courses_rel BY CourseDuration > 4;

course_offering = FILTER Courses_rel BY OfferingMode == 'Online';

STORE course_status INTO '/user/pig_output/course_status.csv' USING PigStorage(',');

STORE course_SAP INTO '/user/pig_output/course_SAP.csv' USING PigStorage(',');

STORE course_duration INTO '/user/pig_output/course_duration.csv' USING PigStorage(',');

STORE course_offering INTO '/user/pig_output/course_offering.csv' USING PigStorage(',');


--Identify the participants enrolled for the most demanding or popular courses 

courses_data = FOREACH Courses_rel GENERATE CourseOwnerId,CourseTitle;

courses_group = GROUP courses_data BY CourseTitle;

top_courses = FOREACH courses_group GENERATE courses_data.CourseOwnerId AS CourseOwnerId,group,COUNT(courses_data) AS popularity;

topcourses_order = ORDER top_courses BY popularity DESC;

topcourses_distinct = DISTINCT topcourses_order;

STORE topcourses_distinct INTO '/user/pig_output/popular_courses.csv' USING PigStorage(',');

--Associate participant details with course details and list the particulars based on various conditions

participant_course_join = JOIN Participant_rel BY ParticipantId,Courses_rel BY CourseOwnerId;

result_data = FOREACH participant_course_join GENERATE
Participant_rel::ParticipantId,Participant_rel::ParticipantMailId,
Participant_rel::ParticipantCountry,Participant_rel::TrainingHours,Participant_rel::Location,Participant_rel::Status,Courses_rel::CourseTitle;
 
filtered_data = FILTER result_data BY TrainingHours > 6;

STORE filtered_data INTO '/user/pig_output/training_duration.csv' USING PigStorage(',');

--Compute the count of participants based on country 

CountryParticipant= GROUP Participant_rel BY ParticipantCountry ;

CountryParticipantCount= FOREACH CountryParticipant GENERATE group,COUNT(Participant_rel);

STORE CountryParticipantCount INTO '/user/pig_output/participant_countries.csv' USING PigStorage(',');

--Compute the average training hours of each course 

CourseTitle = GROUP Courses_rel BY CourseTitle;

AvgCourseHours = FOREACH (GROUP Courses_rel BY CourseTitle) GENERATE group AS course,AVG(Courses_rel.CourseDuration);

STORE AvgCourseHours INTO '/user/pig_output/avg_training_duration.csv' USING PigStorage(',');

--User Defined Funtion to replace Blank/NULL instructor IDs with valid ID

instructorId_is_null = FILTER Participant_rel BY InstructorId is null;

REGISTER /root/Desktop/big-data-analytics-rwc/Pig/IDValidator.jar;

DEFINE ValidateInstructorUDF com.course.validation.InstructorValidator();

instructor_validation = FOREACH instructorId_is_null GENERATE ParticipantId,
ParticipantMailId,
ParticipantCountry,
ValidateInstructorUDF(OfferingMode) AS InstructorId,
CourseCode,
CourseOfferingId,
Source,
DateActionPerformed,
TrainingHours,
Location,
OfferingMode,
Status,
TrackName;

STORE instructor_validation INTO '/user/pig_output/instructor_validation.csv' USING PigStorage(',');


