# Summary
- Test: sqlite3/test/date.test
- Repro: `testfixture test/date.test`
- Failing cases: date-1.12, date-1.19, date-1.20, date-1.21, date-2.2b, date-2.24, date-2.40, date-2.60, date-3.2.2, date-3.5, date-4.1, date-5.5, date-6.1, date-6.2, date-6.3, date-6.4, date-6.5, date-6.6, date-6.7, date-6.8, date-6.9, date-6.10, date-6.11, date-6.12, date-6.20, date-6.21, date-6.22, date-6.23, date-6.24, date-6.28, date-6.29, date-6.31, date-6.32, date-8.1, date-8.2, date-8.3, date-8.4, date-8.5, date-8.6, date-8.7, date-8.8, date-8.9, date-8.10, date-8.11, date-8.12, date-8.13, date-8.14, date-8.15, date-8.16, date-8.17, date-8.18, date-8.19, date-11.1, date-11.2, date-11.3, date-11.4, date-11.5, date-11.6, date-11.7, date-11.8, date-11.9, date-13.21, date-13.22, date-13.23, date-13.24, date-13.30, date-13.31, date-13.32, date-13.33, date-13.34, date-13.36, date-13.37, date-14.1, date-14.2.0, date-14.2.1, date-14.2.2, date-14.2.3, date-14.2.4, date-14.2.5, date-14.2.6, date-14.2.7, date-14.2.8, date-14.2.9, date-14.2.10, date-14.2.11, date-14.2.12, date-14.2.13, date-14.2.14, date-14.2.15, date-14.2.16, date-14.2.17, date-14.2.18, date-14.2.19, date-14.2.20, date-14.2.21, date-14.2.22, date-14.2.23, date-14.2.24, date-14.2.25, date-14.2.26, date-14.2.27, date-14.2.28, date-14.2.29, date-14.2.30, date-14.2.31, date-14.2.32, date-14.2.33, date-14.2.34, date-14.2.35, date-14.2.36, date-14.2.37, date-14.2.38, date-14.2.39, date-14.2.40, date-14.2.41, date-14.2.42, date-14.2.43, date-14.2.44, date-14.2.45, date-14.2.46, date-14.2.47, date-14.2.48, date-14.2.49, date-14.2.50, date-14.2.51, date-14.2.52, date-14.2.53, date-14.2.54, date-14.2.55, date-14.2.56, date-14.2.57, date-14.2.58, date-14.2.59, date-14.2.60, date-14.2.61, date-14.2.62, date-14.2.63, date-14.2.64, date-14.2.65, date-14.2.66, date-14.2.67, date-14.2.68, date-14.2.69, date-14.2.70, date-14.2.71, date-14.2.72, date-14.2.73, date-14.2.74, date-14.2.75, date-14.2.76, date-14.2.77, date-14.2.78, date-14.2.79, date-14.2.80, date-14.2.81, date-14.2.82, date-14.2.83, date-14.2.84, date-14.2.85, date-14.2.86, date-14.2.87, date-14.2.88, date-14.2.89, date-14.2.90, date-14.2.91, date-14.2.92, date-14.2.93, date-14.2.94, date-14.2.95, date-14.2.96, date-14.2.97, date-14.2.98, date-14.2.99, date-14.2.100, date-14.2.101, date-14.2.102, date-14.2.103, date-14.2.104, date-14.2.105, date-14.2.106, date-14.2.107, date-14.2.108, date-14.2.109, date-14.2.110, date-14.2.111, date-14.2.112, date-14.2.113, date-14.2.114, date-14.2.115, date-14.2.116, date-14.2.117, date-14.2.118, date-14.2.119, date-14.2.120, date-14.2.121, date-14.2.122, date-14.2.123, date-14.2.124, date-14.2.125, date-14.2.126, date-14.2.127, date-14.2.128, date-14.2.129, date-14.2.130, date-14.2.131, date-14.2.132, date-14.2.133, date-14.2.134, date-14.2.135, date-14.2.136, date-14.2.137, date-14.2.138, date-14.2.139, date-14.2.140, date-14.2.141, date-14.2.142, date-14.2.143, date-14.2.144, date-14.2.145, date-14.2.146, date-14.2.147, date-14.2.148, date-14.2.149, date-14.2.150, date-14.2.151, date-14.2.152, date-14.2.153, date-14.2.154, date-14.2.155, date-14.2.156, date-14.2.157, date-14.2.158, date-14.2.159, date-14.2.160, date-14.2.161, date-14.2.162, date-14.2.163, date-14.2.164, date-14.2.165, date-14.2.166, date-14.2.167, date-14.2.168, date-14.2.169, date-14.2.170, date-14.2.171, date-14.2.172, date-14.2.173, date-14.2.174, date-14.2.175, date-14.2.176, date-14.2.177, date-14.2.178, date-14.2.179, date-14.2.180, date-14.2.181, date-14.2.182, date-14.2.183, date-14.2.184, date-14.2.185, date-14.2.186, date-14.2.187, date-14.2.188, date-14.2.189, date-14.2.190, date-14.2.191, date-14.2.192, date-14.2.193, date-14.2.194, date-14.2.195, date-14.2.196, date-14.2.197, date-14.2.198, date-14.2.199, date-14.2.200, date-14.2.201, date-14.2.202, date-14.2.203, date-14.2.204, date-14.2.205, date-14.2.206, date-14.2.207, date-14.2.208, date-14.2.209, date-14.2.210, date-14.2.211, date-14.2.212, date-14.2.213, date-14.2.214, date-14.2.215, date-14.2.216, date-14.2.217, date-14.2.218, date-14.2.219, date-14.2.220, date-14.2.221, date-14.2.222, date-14.2.223, date-14.2.224, date-14.2.225, date-14.2.226, date-14.2.227, date-14.2.228, date-14.2.229, date-14.2.230, date-14.2.231, date-14.2.232, date-14.2.233, date-14.2.234, date-14.2.235, date-14.2.236, date-14.2.237, date-14.2.238, date-14.2.239, date-14.2.240, date-14.2.241, date-14.2.242, date-14.2.243, date-14.2.244, date-14.2.245, date-14.2.246, date-14.2.247, date-14.2.248, date-14.2.249, date-14.2.250, date-14.2.251, date-14.2.252, date-14.2.253, date-14.2.254, date-14.2.255, date-15.1, date-15.2, date-16.1, date-16.5, date-16.7, date-16.9, date-16.11, date-16.13, date-16.15, date-16.17, date-16.21, date-16.23, date-16.25, date-16.27, date-16.28, date-16.29, date-16.31, date-17.6, date-18.2, date-18.3, date-18.4, date-18.5, date-19.1, date-19.2a, date-19.2b, date-19.2c, date-19.3, date-19.4, date-19.5, date-19.6, date-19.7, date-19.8, date-19.9, date-19.10, date-19.11, date-19.12, date-19.21, date-19.22a, date-19.22b, date-19.22c, date-19.23, date-19.24, date-19.25, date-19.26, date-19.27, date-19.28, date-19.29, date-19.30, date-19.31, date-19.32, date-19.40, date-19.41, date-19.42, date-19.43, date-19.44, date-19.45, date-19.46, date-19.47, date-19.48, date-19.49, date-19.50, date-19.51, date-19.52, date-19.53
- Primary errors: ! date-1.12 expected: [2452701.5] | ! date-1.12 got:      [NULL] | ! date-1.19 expected: [2451545.00000116]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-date-3641417-1769533960745
DEBUG: tester.tcl sourced, db=db
Running date.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/date.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/date.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
date-1.1... Ok
date-1.2... Ok
date-1.3... Ok
date-1.4... Ok
date-1.5... Ok
date-1.6... Ok
date-1.7... Ok
date-1.8... Ok
date-1.9... Ok
date-1.10... Ok
date-1.11... Ok
date-1.12...
! date-1.12 expected: [2452701.5]
! date-1.12 got:      [NULL]
date-1.13... Ok
date-1.14... Ok
date-1.15... Ok
date-1.16... Ok
date-1.17... Ok
date-1.18.1... Ok
date-1.18.2... Ok
date-1.18.3... Ok
date-1.18.4... Ok
date-1.18.4... Ok
date-1.19...
! date-1.19 expected: [2451545.00000116]
! date-1.19 got:      [2451545.0000011576]
date-1.20...
! date-1.20 expected: [2451545.00000012]
! date-1.20 got:      [2451545.000000116]
date-1.21...
! date-1.21 expected: [2451545.00000001]
! date-1.21 got:      [2451545.0000000116]
date-1.22... Ok
date-1.23... Ok
date-1.23b... Ok
date-1.24... Ok
date-1.25... Ok
date-1.26... Ok
date-1.27... Ok
date-1.28... Ok
date-1.29... Ok
date-2.1... Ok
date-2.1b... Ok
date-2.1c... Ok
date-2.1d... Ok
date-2.2... Ok
date-2.2b...
! date-2.2b expected: [{2000-01-01 00:00:00}]
! date-2.2b got:      [NULL]
date-2.2c-0... Ok
date-2.2c-1... Ok
date-2.2c-2... Ok
date-2.2c-3... Ok
date-2.2c-4... Ok
date-2.2c-5... Ok
date-2.2c-6... Ok
date-2.2c-7... Ok
date-2.2c-8... Ok
date-2.2c-9... Ok
date-2.2c-10... Ok
date-2.2c-11... Ok
date-2.2c-12... Ok
date-2.2c-13... Ok
date-2.2c-14... Ok
date-2.2c-15... Ok
date-2.2c-16... Ok
date-2.2c-17... Ok
date-2.2c-18... Ok
date-2.2c-19... Ok
date-2.2c-20... Ok
date-2.2c-21... Ok
date-2.2c-22... Ok
date-2.2c-23... Ok
date-2.2c-24... Ok
date-2.2c-25... Ok
date-2.2c-26... Ok
date-2.2c-27... Ok
date-2.2c-28... Ok
date-2.2c-29... Ok
date-2.2c-30... Ok
date-2.2c-31... Ok
date-2.2c-32... Ok
date-2.2c-33... Ok
date-2.2c-34... Ok
date-2.2c-35... Ok
date-2.2c-36... Ok
date-2.2c-37... Ok
date-2.2c-38... Ok
date-2.2c-39... Ok
date-2.2c-40... Ok
date-2.2c-41... Ok
date-2.2c-42... Ok
date-2.2c-43... Ok
date-2.2c-44... Ok
date-2.2c-45... Ok
date-2.2c-46... Ok
date-2.2c-47... Ok
date-2.2c-48... Ok
date-2.2c-49... Ok
date-2.2c-50... Ok
date-2.2c-51... Ok
date-2.2c-52... Ok
date-2.2c-53... Ok
date-2.2c-54... Ok
date-2.2c-55... Ok
date-2.2c-56... Ok
date-2.2c-57... Ok
date-2.2c-58... Ok
date-2.2c-59... Ok
date-2.2c-60... Ok
date-2.2c-61... Ok
date-2.2c-62... Ok
date-2.2c-63... Ok
date-2.2c-64... Ok
date-2.2c-65... Ok
date-2.2c-66... Ok
date-2.2c-67... Ok
date-2.2c-68... Ok
date-2.2c-69... Ok
date-2.2c-70... Ok
date-2.2c-71... Ok
date-2.2c-72... Ok
date-2.2c-73... Ok
date-2.2c-74... Ok
date-2.2c-75... Ok
date-2.2c-76... Ok
date-2.2c-77... Ok
date-2.2c-78... Ok
date-2.2c-79... Ok
date-2.2c-80... Ok
date-2.2c-81... Ok
date-2.2c-82... Ok
date-2.2c-83... Ok
date-2.2c-84... Ok
date-2.2c-85... Ok
date-2.2c-86... Ok
date-2.2c-87... Ok
date-2.2c-88... Ok
date-2.2c-89... Ok
date-2.2c-90... Ok
date-2.2c-91... Ok
date-2.2c-92... Ok
date-2.2c-93... Ok
date-2.2c-94... Ok
date-2.2c-95... Ok
date-2.2c-96... Ok
date-2.2c-97... Ok
date-2.2c-98... Ok
date-2.2c-99... Ok
date-2.2c-100... Ok
date-2.2c-101... Ok
date-2.2c-102... Ok
date-2.2c-103... Ok
date-2.2c-104... Ok
date-2.2c-105... Ok
date-2.2c-106... Ok
date-2.2c-107... Ok
date-2.2c-108... Ok
date-2.2c-109... Ok
date-2.2c-110... Ok
date-2.2c-111... Ok
date-2.2c-112... Ok
date-2.2c-113... Ok
date-2.2c-114... Ok
date-2.2c-115... Ok
date-2.2c-116... Ok
date-2.2c-117... Ok
date-2.2c-118... Ok
date-2.2c-119... Ok
date-2.2c-120... Ok
date-2.2c-121... Ok
date-2.2c-122... Ok
date-2.2c-123... Ok
date-2.2c-124... Ok
date-2.2c-125... Ok
date-2.2c-126... Ok
date-2.2c-127... Ok
date-2.2c-128... Ok
date-2.2c-129... Ok
date-2.2c-130... Ok
date-2.2c-131... Ok
date-2.2c-132... Ok
date-2.2c-133... Ok
date-2.2c-134... Ok
date-2.2c-135... Ok
date-2.2c-136... Ok
date-2.2c-137... Ok
date-2.2c-138... Ok
date-2.2c-139... Ok
date-2.2c-140... Ok
date-2.2c-141... Ok
date-2.2c-142... Ok
date-2.2c-143... Ok
date-2.2c-144... Ok
date-2.2c-145... Ok
date-2.2c-146... Ok
date-2.2c-147... Ok
date-2.2c-148... Ok
date-2.2c-149... Ok
date-2.2c-150... Ok
date-2.2c-151... Ok
date-2.2c-152... Ok
date-2.2c-153... Ok
date-2.2c-154... Ok
date-2.2c-155... Ok
date-2.2c-156... Ok
date-2.2c-157... Ok
date-2.2c-158... Ok
date-2.2c-159... Ok
date-2.2c-160... Ok
date-2.2c-161... Ok
date-2.2c-162... Ok
date-2.2c-163... Ok
date-2.2c-164... Ok
date-2.2c-165... Ok
date-2.2c-166... Ok
date-2.2c-167... Ok
date-2.2c-168... Ok
date-2.2c-169... Ok
date-2.2c-170... Ok
date-2.2c-171... Ok
date-2.2c-172... Ok
date-2.2c-173... Ok
date-2.2c-174... Ok
date-2.2c-175... Ok
date-2.2c-176... Ok
date-2.2c-177... Ok
date-2.2c-178... Ok
date-2.2c-179... Ok
date-2.2c-180... Ok
date-2.2c-181... Ok
date-2.2c-182... Ok
date-2.2c-183... Ok
date-2.2c-184... Ok
date-2.2c-185... Ok
date-2.2c-186... Ok
date-2.2c-187... Ok
date-2.2c-188... Ok
date-2.2c-189... Ok
date-2.2c-190... Ok
date-2.2c-191... Ok
date-2.2c-192... Ok
date-2.2c-193... Ok
date-2.2c-194... Ok
date-2.2c-195... Ok
date-2.2c-196... Ok
date-2.2c-197... Ok
date-2.2c-198... Ok
date-2.2c-199... Ok
date-2.2c-200... Ok
date-2.2c-201... Ok
date-2.2c-202... Ok
date-2.2c-203... Ok
date-2.2c-204... Ok
date-2.2c-205... Ok
date-2.2c-206... Ok
date-2.2c-207... Ok
date-2.2c-208... Ok
date-2.2c-209... Ok
date-2.2c-210... Ok
date-2.2c-211... Ok
date-2.2c-212... Ok
date-2.2c-213... Ok
date-2.2c-214... Ok
date-2.2c-215... Ok
date-2.2c-216... Ok
date-2.2c-217... Ok
date-2.2c-218... Ok
date-2.2c-219... Ok
date-2.2c-220... Ok
date-2.2c-221... Ok
date-2.2c-222... Ok
date-2.2c-223... Ok
date-2.2c-224... Ok
date-2.2c-225... Ok
date-2.2c-226... Ok
date-2.2c-227... Ok
date-2.2c-228... Ok
date-2.2c-229... Ok
date-2.2c-230... Ok
date-2.2c-231... Ok
date-2.2c-232... Ok
date-2.2c-233... Ok
date-2.2c-234... Ok
date-2.2c-235... Ok
date-2.2c-236... Ok
date-2.2c-237... Ok
date-2.2c-238... Ok
date-2.2c-239... Ok
date-2.2c-240... Ok
date-2.2c-241... Ok
date-2.2c-242... Ok
date-2.2c-243... Ok
date-2.2c-244... Ok
date-2.2c-245... Ok
date-2.2c-246... Ok
date-2.2c-247... Ok
date-2.2c-248... Ok
date-2.2c-249... Ok
date-2.2c-250... Ok
date-2.2c-251... Ok
date-2.2c-252... Ok
date-2.2c-253... Ok
date-2.2c-254... Ok
date-2.2c-255... Ok
date-2.2c-256... Ok
date-2.2c-257... Ok
date-2.2c-258... Ok
date-2.2c-259... Ok
date-2.2c-260... Ok
date-2.2c-261... Ok
date-2.2c-262... Ok
date-2.2c-263... Ok
date-2.2c-264... Ok
date-2.2c-265... Ok
date-2.2c-266... Ok
date-2.2c-267... Ok
date-2.2c-268... Ok
date-2.2c-269... Ok
date-2.2c-270... Ok
date-2.2c-271... Ok
date-2.2c-272... Ok
date-2.2c-273... Ok
date-2.2c-274... Ok
date-2.2c-275... Ok
date-2.2c-276... Ok
date-2.2c-277... Ok
date-2.2c-278... Ok
date-2.2c-279... Ok
date-2.2c-280... Ok
date-2.2c-281... Ok
date-2.2c-282... Ok
date-2.2c-283... Ok
date-2.2c-284... Ok
date-2.2c-285... Ok
date-2.2c-286... Ok
date-2.2c-287... Ok
date-2.2c-288... Ok
date-2.2c-289... Ok
date-2.2c-290... Ok
date-2.2c-291... Ok
date-2.2c-292... Ok
date-2.2c-293... Ok
date-2.2c-294... Ok
date-2.2c-295... Ok
date-2.2c-296... Ok
date-2.2c-297... Ok
date-2.2c-298... Ok
date-2.2c-299... Ok
date-2.2c-300... Ok
date-2.2c-301... Ok
date-2.2c-302... Ok
date-2.2c-303... Ok
date-2.2c-304... Ok
date-2.2c-305... Ok
date-2.2c-306... Ok
date-2.2c-307... Ok
date-2.2c-308... Ok
date-2.2c-309... Ok
date-2.2c-310... Ok
date-2.2c-311... Ok
date-2.2c-312... Ok
date-2.2c-313... Ok
date-2.2c-314... Ok
date-2.2c-315... Ok
date-2.2c-316... Ok
date-2.2c-317... Ok
date-2.2c-318... Ok
date-2.2c-319... Ok
date-2.2c-320... Ok
date-2.2c-321... Ok
date-2.2c-322... Ok
date-2.2c-323... Ok
date-2.2c-324... Ok
date-2.2c-325... Ok
date-2.2c-326... Ok
date-2.2c-327... Ok
date-2.2c-328... Ok
date-2.2c-329... Ok
date-2.2c-330... Ok
date-2.2c-331... Ok
date-2.2c-332... Ok
date-2.2c-333... Ok
date-2.2c-334... Ok
date-2.2c-335... Ok
date-2.2c-336... Ok
date-2.2c-337... Ok
date-2.2c-338... Ok
date-2.2c-339... Ok
date-2.2c-340... Ok
date-2.2c-341... Ok
date-2.2c-342... Ok
date-2.2c-343... Ok
date-2.2c-344... Ok
date-2.2c-345... Ok
date-2.2c-346... Ok
date-2.2c-347... Ok
date-2.2c-348... Ok
date-2.2c-349... Ok
date-2.2c-350... Ok
date-2.2c-351... Ok
date-2.2c-352... Ok
date-2.2c-353... Ok
date-2.2c-354... Ok
date-2.2c-355... Ok
date-2.2c-356... Ok
date-2.2c-357... Ok
date-2.2c-358... Ok
date-2.2c-359... Ok
date-2.2c-360... Ok
date-2.2c-361... Ok
date-2.2c-362... Ok
date-2.2c-363... Ok
date-2.2c-364... Ok
date-2.2c-365... Ok
date-2.2c-366... Ok
date-2.2c-367... Ok
date-2.2c-368... Ok
date-2.2c-369... Ok
date-2.2c-370... Ok
date-2.2c-371... Ok
date-2.2c-372... Ok
date-2.2c-373... Ok
date-2.2c-374... Ok
date-2.2c-375... Ok
date-2.2c-376... Ok
date-2.2c-377... Ok
date-2.2c-378... Ok
date-2.2c-379... Ok
date-2.2c-380... Ok
date-2.2c-381... Ok
date-2.2c-382... Ok
date-2.2c-383... Ok
date-2.2c-384... Ok
date-2.2c-385... Ok
date-2.2c-386... Ok
date-2.2c-387... Ok
date-2.2c-388... Ok
date-2.2c-389... Ok
date-2.2c-390... Ok
date-2.2c-391... Ok
date-2.2c-392... Ok
date-2.2c-393... Ok
date-2.2c-394... Ok
date-2.2c-395... Ok
date-2.2c-396... Ok
date-2.2c-397... Ok
date-2.2c-398... Ok
date-2.2c-399... Ok
date-2.2c-400... Ok
date-2.2c-401... Ok
date-2.2c-402... Ok
date-2.2c-403... Ok
date-2.2c-404... Ok
date-2.2c-405... Ok
date-2.2c-406... Ok
date-2.2c-407... Ok
date-2.2c-408... Ok
date-2.2c-409... Ok
date-2.2c-410... Ok
date-2.2c-411... Ok
date-2.2c-412... Ok
date-2.2c-413... Ok
date-2.2c-414... Ok
date-2.2c-415... Ok
date-2.2c-416... Ok
date-2.2c-417... Ok
date-2.2c-418... Ok
date-2.2c-419... Ok
date-2.2c-420... Ok
date-2.2c-421... Ok
date-2.2c-422... Ok
date-2.2c-423... Ok
date-2.2c-424... Ok
date-2.2c-425... Ok
date-2.2c-426... Ok
date-2.2c-427... Ok
date-2.2c-428... Ok
date-2.2c-429... Ok
date-2.2c-430... Ok
date-2.2c-431... Ok
date-2.2c-432... Ok
date-2.2c-433... Ok
date-2.2c-434... Ok
date-2.2c-435... Ok
date-2.2c-436... Ok
date-2.2c-437... Ok
date-2.2c-438... Ok
date-2.2c-439... Ok
date-2.2c-440... Ok
date-2.2c-441... Ok
date-2.2c-442... Ok
date-2.2c-443... Ok
date-2.2c-444... Ok
date-2.2c-445... Ok
date-2.2c-446... Ok
date-2.2c-447... Ok
date-2.2c-448... Ok
date-2.2c-449... Ok
date-2.2c-450... Ok
date-2.2c-451... Ok
date-2.2c-452... Ok
date-2.2c-453... Ok
date-2.2c-454... Ok
date-2.2c-455... Ok
date-2.2c-456... Ok
date-2.2c-457... Ok
date-2.2c-458... Ok
date-2.2c-459... Ok
date-2.2c-460... Ok
date-2.2c-461... Ok
date-2.2c-462... Ok
date-2.2c-463... Ok
date-2.2c-464... Ok
date-2.2c-465... Ok
date-2.2c-466... Ok
date-2.2c-467... Ok
date-2.2c-468... Ok
date-2.2c-469... Ok
date-2.2c-470... Ok
date-2.2c-471... Ok
date-2.2c-472... Ok
date-2.2c-473... Ok
date-2.2c-474... Ok
date-2.2c-475... Ok
date-2.2c-476... Ok
date-2.2c-477... Ok
date-2.2c-478... Ok
date-2.2c-479... Ok
date-2.2c-480... Ok
date-2.2c-481... Ok
date-2.2c-482... Ok
date-2.2c-483... Ok
date-2.2c-484... Ok
date-2.2c-485... Ok
date-2.2c-486... Ok
date-2.2c-487... Ok
date-2.2c-488... Ok
date-2.2c-489... Ok
date-2.2c-490... Ok
date-2.2c-491... Ok
date-2.2c-492... Ok
date-2.2c-493... Ok
date-2.2c-494... Ok
date-2.2c-495... Ok
date-2.2c-496... Ok
date-2.2c-497... Ok
date-2.2c-498... Ok
date-2.2c-499... Ok
date-2.2c-500... Ok
date-2.2c-501... Ok
date-2.2c-502... Ok
date-2.2c-503... Ok
date-2.2c-504... Ok
date-2.2c-505... Ok
date-2.2c-506... Ok
date-2.2c-507... Ok
date-2.2c-508... Ok
date-2.2c-509... Ok
date-2.2c-510... Ok
date-2.2c-511... Ok
date-2.2c-512... Ok
date-2.2c-513... Ok
date-2.2c-514... Ok
date-2.2c-515... Ok
date-2.2c-516... Ok
date-2.2c-517... Ok
date-2.2c-518... Ok
date-2.2c-519... Ok
date-2.2c-520... Ok
date-2.2c-521... Ok
date-2.2c-522... Ok
date-2.2c-523... Ok
date-2.2c-524... Ok
date-2.2c-525... Ok
date-2.2c-526... Ok
date-2.2c-527... Ok
date-2.2c-528... Ok
date-2.2c-529... Ok
date-2.2c-530... Ok
date-2.2c-531... Ok
date-2.2c-532... Ok
date-2.2c-533... Ok
date-2.2c-534... Ok
date-2.2c-535... Ok
date-2.2c-536... Ok
date-2.2c-537... Ok
date-2.2c-538... Ok
date-2.2c-539... Ok
date-2.2c-540... Ok
date-2.2c-541... Ok
date-2.2c-542... Ok
date-2.2c-543... Ok
date-2.2c-544... Ok
date-2.2c-545... Ok
date-2.2c-546... Ok
date-2.2c-547... Ok
date-2.2c-548... Ok
date-2.2c-549... Ok
date-2.2c-550... Ok
date-2.2c-551... Ok
date-2.2c-552... Ok
date-2.2c-553... Ok
date-2.2c-554... Ok
date-2.2c-555... Ok
date-2.2c-556... Ok
date-2.2c-557... Ok
date-2.2c-558... Ok
date-2.2c-559... Ok
date-2.2c-560... Ok
date-2.2c-561... Ok
date-2.2c-562... Ok
date-2.2c-563... Ok
date-2.2c-564... Ok
date-2.2c-565... Ok
date-2.2c-566... Ok
date-2.2c-567... Ok
date-2.2c-568... Ok
date-2.2c-569... Ok
date-2.2c-570... Ok
date-2.2c-571... Ok
date-2.2c-572... Ok
date-2.2c-573... Ok
date-2.2c-574... Ok
date-2.2c-575... Ok
date-2.2c-576... Ok
date-2.2c-577... Ok
date-2.2c-578... Ok
date-2.2c-579... Ok
date-2.2c-580... Ok
date-2.2c-581... Ok
date-2.2c-582... Ok
date-2.2c-583... Ok
date-2.2c-584... Ok
date-2.2c-585... Ok
date-2.2c-586... Ok
date-2.2c-587... Ok
date-2.2c-588... Ok
date-2.2c-589... Ok
date-2.2c-590... Ok
date-2.2c-591... Ok
date-2.2c-592... Ok
date-2.2c-593... Ok
date-2.2c-594... Ok
date-2.2c-595... Ok
date-2.2c-596... Ok
date-2.2c-597... Ok
date-2.2c-598... Ok
date-2.2c-599... Ok
date-2.2c-600... Ok
date-2.2c-601... Ok
date-2.2c-602... Ok
date-2.2c-603... Ok
date-2.2c-604... Ok
date-2.2c-605... Ok
date-2.2c-606... Ok
date-2.2c-607... Ok
date-2.2c-608... Ok
date-2.2c-609... Ok
date-2.2c-610... Ok
date-2.2c-611... Ok
date-2.2c-612... Ok
date-2.2c-613... Ok
date-2.2c-614... Ok
date-2.2c-615... Ok
date-2.2c-616... Ok
date-2.2c-617... Ok
date-2.2c-618... Ok
date-2.2c-619... Ok
date-2.2c-620... Ok
date-2.2c-621... Ok
date-2.2c-622... Ok
date-2.2c-623... Ok
date-2.2c-624... Ok
date-2.2c-625... Ok
date-2.2c-626... Ok
date-2.2c-627... Ok
date-2.2c-628... Ok
date-2.2c-629... Ok
date-2.2c-630... Ok
date-2.2c-631... Ok
date-2.2c-632... Ok
date-2.2c-633... Ok
date-2.2c-634... Ok
date-2.2c-635... Ok
date-2.2c-636... Ok
date-2.2c-637... Ok
date-2.2c-638... Ok
date-2.2c-639... Ok
date-2.2c-640... Ok
date-2.2c-641... Ok
date-2.2c-642... Ok
date-2.2c-643... Ok
date-2.2c-644... Ok
date-2.2c-645... Ok
date-2.2c-646... Ok
date-2.2c-647... Ok
date-2.2c-648... Ok
date-2.2c-649... Ok
date-2.2c-650... Ok
date-2.2c-651... Ok
date-2.2c-652... Ok
date-2.2c-653... Ok
date-2.2c-654... Ok
date-2.2c-655... Ok
date-2.2c-656... Ok
date-2.2c-657... Ok
date-2.2c-658... Ok
date-2.2c-659... Ok
date-2.2c-660... Ok
date-2.2c-661... Ok
date-2.2c-662... Ok
date-2.2c-663... Ok
date-2.2c-664... Ok
date-2.2c-665... Ok
date-2.2c-666... Ok
date-2.2c-667... Ok
date-2.2c-668... Ok
date-2.2c-669... Ok
date-2.2c-670... Ok
date-2.2c-671... Ok
date-2.2c-672... Ok
date-2.2c-673... Ok
date-2.2c-674... Ok
date-2.2c-675... Ok
date-2.2c-676... Ok
date-2.2c-677... Ok
date-2.2c-678... Ok
date-2.2c-679... Ok
date-2.2c-680... Ok
date-2.2c-681... Ok
date-2.2c-682... Ok
date-2.2c-683... Ok
date-2.2c-684... Ok
date-2.2c-685... Ok
date-2.2c-686... Ok
date-2.2c-687... Ok
date-2.2c-688... Ok
date-2.2c-689... Ok
date-2.2c-690... Ok
date-2.2c-691... Ok
date-2.2c-692... Ok
date-2.2c-693... Ok
date-2.2c-694... Ok
date-2.2c-695... Ok
date-2.2c-696... Ok
date-2.2c-697... Ok
date-2.2c-698... Ok
date-2.2c-699... Ok
date-2.2c-700... Ok
date-2.2c-701... Ok
date-2.2c-702... Ok
date-2.2c-703... Ok
date-2.2c-704... Ok
date-2.2c-705... Ok
date-2.2c-706... Ok
date-2.2c-707... Ok
date-2.2c-708... Ok
date-2.2c-709... Ok
date-2.2c-710... Ok
date-2.2c-711... Ok
date-2.2c-712... Ok
date-2.2c-713... Ok
date-2.2c-714... Ok
date-2.2c-715... Ok
date-2.2c-716... Ok
date-2.2c-717... Ok
date-2.2c-718... Ok
date-2.2c-719... Ok
date-2.2c-720... Ok
date-2.2c-721... Ok
date-2.2c-722... Ok
date-2.2c-723... Ok
date-2.2c-724... Ok
date-2.2c-725... Ok
date-2.2c-726... Ok
date-2.2c-727... Ok
date-2.2c-728... Ok
date-2.2c-729... Ok
date-2.2c-730... Ok
date-2.2c-731... Ok
date-2.2c-732... Ok
date-2.2c-733... Ok
date-2.2c-734... Ok
date-2.2c-735... Ok
date-2.2c-736... Ok
date-2.2c-737... Ok
date-2.2c-738... Ok
date-2.2c-739... Ok
date-2.2c-740... Ok
date-2.2c-741... Ok
date-2.2c-742... Ok
date-2.2c-743... Ok
date-2.2c-744... Ok
date-2.2c-745... Ok
date-2.2c-746... Ok
date-2.2c-747... Ok
date-2.2c-748... Ok
date-2.2c-749... Ok
date-2.2c-750... Ok
date-2.2c-751... Ok
date-2.2c-752... Ok
date-2.2c-753... Ok
date-2.2c-754... Ok
date-2.2c-755... Ok
date-2.2c-756... Ok
date-2.2c-757... Ok
date-2.2c-758... Ok
date-2.2c-759... Ok
date-2.2c-760... Ok
date-2.2c-761... Ok
date-2.2c-762... Ok
date-2.2c-763... Ok
date-2.2c-764... Ok
date-2.2c-765... Ok
date-2.2c-766... Ok
date-2.2c-767... Ok
date-2.2c-768... Ok
date-2.2c-769... Ok
date-2.2c-770... Ok
date-2.2c-771... Ok
date-2.2c-772... Ok
date-2.2c-773... Ok
date-2.2c-774... Ok
date-2.2c-775... Ok
date-2.2c-776... Ok
date-2.2c-777... Ok
date-2.2c-778... Ok
date-2.2c-779... Ok
date-2.2c-780... Ok
date-2.2c-781... Ok
date-2.2c-782... Ok
date-2.2c-783... Ok
date-2.2c-784... Ok
date-2.2c-785... Ok
date-2.2c-786... Ok
date-2.2c-787... Ok
date-2.2c-788... Ok
date-2.2c-789... Ok
date-2.2c-790... Ok
date-2.2c-791... Ok
date-2.2c-792... Ok
date-2.2c-793... Ok
date-2.2c-794... Ok
date-2.2c-795... Ok
date-2.2c-796... Ok
date-2.2c-797... Ok
date-2.2c-798... Ok
date-2.2c-799... Ok
date-2.2c-800... Ok
date-2.2c-801... Ok
date-2.2c-802... Ok
date-2.2c-803... Ok
date-2.2c-804... Ok
date-2.2c-805... Ok
date-2.2c-806... Ok
date-2.2c-807... Ok
date-2.2c-808... Ok
date-2.2c-809... Ok
date-2.2c-810... Ok
date-2.2c-811... Ok
date-2.2c-812... Ok
date-2.2c-813... Ok
date-2.2c-814... Ok
date-2.2c-815... Ok
date-2.2c-816... Ok
date-2.2c-817... Ok
date-2.2c-818... Ok
date-2.2c-819... Ok
date-2.2c-820... Ok
date-2.2c-821... Ok
date-2.2c-822... Ok
date-2.2c-823... Ok
date-2.2c-824... Ok
date-2.2c-825... Ok
date-2.2c-826... Ok
date-2.2c-827... Ok
date-2.2c-828... Ok
date-2.2c-829... Ok
date-2.2c-830... Ok
date-2.2c-831... Ok
date-2.2c-832... Ok
date-2.2c-833... Ok
date-2.2c-834... Ok
date-2.2c-835... Ok
date-2.2c-836... Ok
date-2.2c-837... Ok
date-2.2c-838... Ok
date-2.2c-839... Ok
date-2.2c-840... Ok
date-2.2c-841... Ok
date-2.2c-842... Ok
date-2.2c-843... Ok
date-2.2c-844... Ok
date-2.2c-845... Ok
date-2.2c-846... Ok
date-2.2c-847... Ok
date-2.2c-848... Ok
date-2.2c-849... Ok
date-2.2c-850... Ok
date-2.2c-851... Ok
date-2.2c-852... Ok
date-2.2c-853... Ok
date-2.2c-854... Ok
date-2.2c-855... Ok
date-2.2c-856... Ok
date-2.2c-857... Ok
date-2.2c-858... Ok
date-2.2c-859... Ok
date-2.2c-860... Ok
date-2.2c-861... Ok
date-2.2c-862... Ok
date-2.2c-863... Ok
date-2.2c-864... Ok
date-2.2c-865... Ok
date-2.2c-866... Ok
date-2.2c-867... Ok
date-2.2c-868... Ok
date-2.2c-869... Ok
date-2.2c-870... Ok
date-2.2c-871... Ok
date-2.2c-872... Ok
date-2.2c-873... Ok
date-2.2c-874... Ok
date-2.2c-875... Ok
date-2.2c-876... Ok
date-2.2c-877... Ok
date-2.2c-878... Ok
date-2.2c-879... Ok
date-2.2c-880... Ok
date-2.2c-881... Ok
date-2.2c-882... Ok
date-2.2c-883... Ok
date-2.2c-884... Ok
date-2.2c-885... Ok
date-2.2c-886... Ok
date-2.2c-887... Ok
date-2.2c-888... Ok
date-2.2c-889... Ok
date-2.2c-890... Ok
date-2.2c-891... Ok
date-2.2c-892... Ok
date-2.2c-893... Ok
date-2.2c-894... Ok
date-2.2c-895... Ok
date-2.2c-896... Ok
date-2.2c-897... Ok
date-2.2c-898... Ok
date-2.2c-899... Ok
date-2.2c-900... Ok
date-2.2c-901... Ok
date-2.2c-902... Ok
date-2.2c-903... Ok
date-2.2c-904... Ok
date-2.2c-905... Ok
date-2.2c-906... Ok
date-2.2c-907... Ok
date-2.2c-908... Ok
date-2.2c-909... Ok
date-2.2c-910... Ok
date-2.2c-911... Ok
date-2.2c-912... Ok
date-2.2c-913... Ok
date-2.2c-914... Ok
date-2.2c-915... Ok
date-2.2c-916... Ok
date-2.2c-917... Ok
date-2.2c-918... Ok
date-2.2c-919... Ok
date-2.2c-920... Ok
date-2.2c-921... Ok
date-2.2c-922... Ok
date-2.2c-923... Ok
date-2.2c-924... Ok
date-2.2c-925... Ok
date-2.2c-926... Ok
date-2.2c-927... Ok
date-2.2c-928... Ok
date-2.2c-929... Ok
date-2.2c-930... Ok
date-2.2c-931... Ok
date-2.2c-932... Ok
date-2.2c-933... Ok
date-2.2c-934... Ok
date-2.2c-935... Ok
date-2.2c-936... Ok
date-2.2c-937... Ok
date-2.2c-938... Ok
date-2.2c-939... Ok
date-2.2c-940... Ok
date-2.2c-941... Ok
date-2.2c-942... Ok
date-2.2c-943... Ok
date-2.2c-944... Ok
date-2.2c-945... Ok
date-2.2c-946... Ok
date-2.2c-947... Ok
date-2.2c-948... Ok
date-2.2c-949... Ok
date-2.2c-950... Ok
date-2.2c-951... Ok
date-2.2c-952... Ok
date-2.2c-953... Ok
date-2.2c-954... Ok
date-2.2c-955... Ok
date-2.2c-956... Ok
date-2.2c-957... Ok
date-2.2c-958... Ok
date-2.2c-959... Ok
date-2.2c-960... Ok
date-2.2c-961... Ok
date-2.2c-962... Ok
date-2.2c-963... Ok
date-2.2c-964... Ok
date-2.2c-965... Ok
date-2.2c-966... Ok
date-2.2c-967... Ok
date-2.2c-968... Ok
date-2.2c-969... Ok
date-2.2c-970... Ok
date-2.2c-971... Ok
date-2.2c-972... Ok
date-2.2c-973... Ok
date-2.2c-974... Ok
date-2.2c-975... Ok
date-2.2c-976... Ok
date-2.2c-977... Ok
date-2.2c-978... Ok
date-2.2c-979... Ok
date-2.2c-980... Ok
date-2.2c-981... Ok
date-2.2c-982... Ok
date-2.2c-983... Ok
date-2.2c-984... Ok
date-2.2c-985... Ok
date-2.2c-986... Ok
date-2.2c-987... Ok
date-2.2c-988... Ok
date-2.2c-989... Ok
date-2.2c-990... Ok
date-2.2c-991... Ok
date-2.2c-992... Ok
date-2.2c-993... Ok
date-2.2c-994... Ok
date-2.2c-995... Ok
date-2.2c-996... Ok
date-2.2c-997... Ok
date-2.2c-998... Ok
date-2.2c-999... Ok
date-2.3... Ok
date-2.4... Ok
date-2.4a... Ok
date-2.4b... Ok
date-2.4c... Ok
date-2.4d... Ok
date-2.4e... Ok
date-2.5... Ok
date-2.6... Ok
date-2.7... Ok
date-2.8... Ok
date-2.9... Ok
date-2.10... Ok
date-2.11... Ok
date-2.12... Ok
date-2.13... Ok
date-2.14... Ok
date-2.15... Ok
date-2.15a... Ok
date-2.15b... Ok
date-2.16... Ok
date-2.17... Ok
date-2.18... Ok
date-2.19... Ok
date-2.20... Ok
date-2.21... Ok
date-2.22... Ok
date-2.23... Ok
date-2.24...
! date-2.24 expected: [{2003-12-07 12:34:00}]
! date-2.24 got:      [{2003-11-22 12:34:00}]
date-2.25... Ok
date-2.26... Ok
date-2.27... Ok
date-2.28... Ok
date-2.29... Ok
date-2.30... Ok
date-2.31... Ok
date-2.32... Ok
date-2.33... Ok
date-2.34... Ok
date-2.35... Ok
date-2.36... Ok
date-2.37... Ok
date-2.38... Ok
date-2.39... Ok
date-2.40...
! date-2.40 expected: [{2008-01-02 03:04:05}]
! date-2.40 got:      [NULL]
date-2.41... Ok
date-2.42... Ok
date-2.43... Ok
date-2.44... Ok
date-2.45... Ok
date-2.46... Ok
date-2.47... Ok
date-2.48... Ok
date-2.49... Ok
date-2.50... Ok
date-2.51... Ok
date-2.60...
! date-2.60 expected: [{2023-03-03 00:00:00}]
! date-2.60 got:      [NULL]
date-3.1... Ok
date-3.2.1... Ok
date-3.2.2...
! date-3.2.2 expected: [59.999]
! date-3.2.2 got:      [60.000]
date-3.3... Ok
date-3.4... Ok
date-3.5...
! date-3.5 expected: [2452944.024264259]
! date-3.5 got:      [2452944.0242642592638731]
date-3.6... Ok
date-3.7... Ok
date-3.8.1... Ok
date-3.8.2... Ok
date-3.8.3... Ok
date-3.8.4... Ok
date-3.8.5... Ok
date-3.8.6... Ok
date-3.8.7... Ok
date-3.8.8... Ok
date-3.8.9... Ok
date-3.9... Ok
date-3.10... Ok
date-3.11.1... Ok
date-3.11.2... Ok
date-3.11.3... Ok
date-3.11.4... Ok
date-3.11.5... Ok
date-3.11.6... Ok
date-3.11.7... Ok
date-3.11.8... Ok
date-3.11.9... Ok
date-3.11.10... Ok
date-3.11.11... Ok
date-3.11.12... Ok
date-3.11.13... Ok
date-3.11.14... Ok
date-3.11.15... Ok
date-3.11.16... Ok
date-3.11.17... Ok
date-3.11.18... Ok
date-3.11.19... Ok
date-3.11.20... Ok
date-3.11.21... Ok
date-3.11.22... Ok
date-3.11.23... Ok
date-3.11.24... Ok
date-3.11.25... Ok
date-3.11.99... Ok
date-3.12... Ok
date-3.13... Ok
date-3.14... Ok
date-3.15... Ok
date-3.16... Ok
date-3.17... Ok
date-3.18.a... Ok
date-3.18.b... Ok
date-3.18.c... Ok
date-3.18.h... Ok
date-3.18.i... Ok
date-3.18.n... Ok
date-3.18.o... Ok
date-3.18.q... Ok
date-3.18.r... Ok
date-3.18.t... Ok
date-3.18.v... Ok
date-3.18.x... Ok
date-3.18.y... Ok
date-3.18.z... Ok
date-3.18.A... Ok
date-3.18.B... Ok
date-3.18.C... Ok
date-3.18.D... Ok
date-3.18.E... Ok
date-3.18.K... Ok
date-3.18.L... Ok
date-3.18.N... Ok
date-3.18.O... Ok
date-3.18.Q... Ok
date-3.18.Z... Ok
date-3.18.0... Ok
date-3.18.1... Ok
date-3.18.2... Ok
date-3.18.3... Ok
date-3.18.4... Ok
date-3.18.5... Ok
date-3.18.6... Ok
date-3.18.6... Ok
date-3.18.7... Ok
date-3.18.9... Ok
date-3.18._... Ok
date-3.20... Ok
date-3.21... Ok
date-3.22... Ok
date-3.23... Ok
date-3.24... Ok
date-3.25... Ok
date-3.26... Ok
date-3.27... Ok
date-3.28... Ok
date-3.29... Ok
date-3.30... Ok
date-3.31... Ok
date-3.32... Ok
date-3.33... Ok
date-3.34... Ok
date-3.35... Ok
date-3.36... Ok
date-3.37... Ok
date-3.40... Ok
date-4.1...
! date-4.1 expected: [2006-09-01]
! date-4.1 got:      [2026-01-27]
date-5.1... Ok
date-5.2... Ok
date-5.3... Ok
date-5.4... Ok
date-5.5...
! date-5.5 expected: [NULL]
! date-5.5 got:      [{1994-04-17 02:00:00}]
date-5.6... Ok
date-5.7... Ok
date-5.8... Ok
date-5.9... Ok
date-5.10... Ok
date-5.11... Ok
date-5.12... Ok
date-5.13... Ok
date-5.14... Ok
date-5.15... Ok
date-6.1...
! date-6.1 expected: [{2000-10-29 12:30:00}]
! date-6.1 got:      [{2000-10-29 07:00:00}]
date-6.2...
! date-6.2 expected: [{2000-10-29 12:00:00}]
! date-6.2 got:      [{2000-10-29 17:30:00}]
date-6.3...
! date-6.3 expected: [{2000-10-30 11:30:00}]
! date-6.3 got:      [{2000-10-30 07:00:00}]
date-6.4...
! date-6.4 expected: [{2000-10-30 12:00:00}]
! date-6.4 got:      [{2000-10-30 16:30:00}]
date-6.5...
! date-6.5 expected: [{2000-10-28 23:29:59}]
! date-6.5 got:      [{2000-10-28 19:59:59}]
date-6.6...
! date-6.6 expected: [{2000-10-29 00:30:00}]
! date-6.6 got:      [{2000-10-28 20:00:00}]
date-6.7...
! date-6.7 expected: [{2000-10-28 23:40:00}]
! date-6.7 got:      [{2000-10-29 04:10:00}]
date-6.8...
! date-6.8 expected: [{2022-02-11 00:29:59}]
! date-6.8 got:      [{2022-02-10 18:59:59}]
date-6.9...
! date-6.9 expected: [{2022-02-10 23:30:00}]
! date-6.9 got:      [{2022-02-10 19:00:00}]
date-6.10...
! date-6.10 expected: [{2022-02-11 00:15:00}]
! date-6.10 got:      [{2022-02-10 18:45:00}]
date-6.11...
! date-6.11 expected: [{2022-02-11 00:15:00}]
! date-6.11 got:      [{2022-02-10 19:45:00}]
date-6.12...
! date-6.12 expected: [{2022-02-11 00:45:00}]
! date-6.12 got:      [{2022-02-11 05:15:00}]
date-6.20...
! date-6.20 expected: [1 {local time unavailable}]
! date-6.20 got:      [0 {{2000-05-29 10:16:00}}]
date-6.21...
! date-6.21 expected: [{1800-10-29 12:30:00}]
! date-6.21 got:      [{1800-10-29 07:03:58}]
date-6.22...
! date-6.22 expected: [{1800-10-29 12:00:00}]
! date-6.22 got:      [{1800-10-29 17:26:02}]
date-6.23...
! date-6.23 expected: [{3000-10-30 11:30:00}]
! date-6.23 got:      [{3000-10-30 08:00:00}]
date-6.24...
! date-6.24 expected: [{3000-10-30 12:00:00}]
! date-6.24 got:      [{3000-10-30 15:30:00}]
date-6.25.1... Ok
date-6.25.2... Ok
date-6.25.3... Ok
date-6.25.4... Ok
date-6.25.5... Ok
date-6.25.6... Ok
date-6.25.7... Ok
date-6.26... Ok
date-6.27... Ok
date-6.28...
! date-6.28 expected: [{2000-10-29 12:30:00}]
! date-6.28 got:      [{2000-10-29 07:00:00}]
date-6.29...
! date-6.29 expected: [{2000-10-29 12:30:00}]
! date-6.29 got:      [{2000-10-29 07:00:00}]
date-6.30... Ok
date-6.31...
! date-6.31 expected: [{2000-10-29 12:30:00}]
! date-6.31 got:      [{2000-10-29 07:00:00}]
date-6.32...
! date-6.32 expected: [{2000-10-29 12:30:00}]
! date-6.32 got:      [{2000-10-29 07:00:00}]
date-7.1... Ok
date-7.2... Ok
date-7.3... Ok
date-7.4... Ok
date-7.5... Ok
date-7.6... Ok
date-7.7... Ok
date-7.8... Ok
date-7.9... Ok
date-7.10... Ok
date-7.11... Ok
date-7.12... Ok
date-7.13... Ok
date-7.14... Ok
date-7.15... Ok
date-7.16... Ok
date-8.1...
! date-8.1 expected: [{2003-10-26 12:34:00}]
! date-8.1 got:      [{2026-02-01 17:12:40}]
date-8.2...
! date-8.2 expected: [{2003-10-27 12:34:00}]
! date-8.2 got:      [{2026-02-02 17:12:40}]
date-8.3...
! date-8.3 expected: [{2003-10-28 12:34:00}]
! date-8.3 got:      [{2026-01-27 17:12:40}]
date-8.4...
! date-8.4 expected: [{2003-10-22 12:34:00}]
! date-8.4 got:      [{2026-01-28 17:12:40}]
date-8.5...
! date-8.5 expected: [{2003-10-01 00:00:00}]
! date-8.5 got:      [{2026-01-01 00:00:00}]
date-8.6...
! date-8.6 expected: [{2003-01-01 00:00:00}]
! date-8.6 got:      [{2026-01-01 00:00:00}]
date-8.7...
! date-8.7 expected: [{2003-10-22 00:00:00}]
! date-8.7 got:      [{2026-01-27 00:00:00}]
date-8.8...
! date-8.8 expected: [{2003-10-23 12:34:00}]
! date-8.8 got:      [{2026-01-28 17:12:40}]
date-8.9...
! date-8.9 expected: [{2003-10-23 12:34:00}]
! date-8.9 got:      [{2026-01-28 17:12:40}]
date-8.10...
! date-8.10 expected: [{2003-10-23 18:34:00}]
! date-8.10 got:      [{2026-01-28 23:12:40}]
date-8.11...
! date-8.11 expected: [{2003-10-21 12:34:00}]
! date-8.11 got:      [{2026-01-26 17:12:40}]
date-8.12...
! date-8.12 expected: [{2003-11-22 12:34:00}]
! date-8.12 got:      [{2026-02-27 17:12:40}]
date-8.13...
! date-8.13 expected: [{2004-09-22 12:34:00}]
! date-8.13 got:      [{2026-12-27 17:12:40}]
date-8.14...
! date-8.14 expected: [{2002-09-22 12:34:00}]
! date-8.14 got:      [{2024-12-27 17:12:40}]
date-8.15...
! date-8.15 expected: [{2003-12-07 12:34:00}]
! date-8.15 got:      [{2026-02-27 17:12:40}]
date-8.16...
! date-8.16 expected: [{1998-10-22 12:34:00}]
! date-8.16 got:      [{2021-01-27 17:12:40}]
date-8.17...
! date-8.17 expected: [{2003-10-22 12:44:30}]
! date-8.17 got:      [{2026-01-27 17:23:10}]
date-8.18...
! date-8.18 expected: [{2003-10-22 11:19:00}]
! date-8.18 got:      [{2026-01-27 15:57:40}]
date-8.19...
! date-8.19 expected: [{2003-10-22 12:34:11}]
! date-8.19 got:      [{2026-01-27 17:12:52}]
date-8.90... Ok
date-9.1... Ok
date-9.2... Ok
date-9.3... Ok
date-9.4... Ok
date-9.5... Ok
date-9.6... Ok
date-9.7... Ok
date-10.1... Ok
date-10.2... Ok
date-10.3... Ok
date-11.1...
! date-11.1 expected: [{2004-02-28 18:39:30}]
! date-11.1 got:      [NULL]
date-11.2...
! date-11.2 expected: [{2004-02-29 08:30:00}]
! date-11.2 got:      [NULL]
date-11.3...
! date-11.3 expected: [{2004-02-29 08:30:00}]
! date-11.3 got:      [NULL]
date-11.4...
! date-11.4 expected: [{2004-02-29 08:30:00}]
! date-11.4 got:      [NULL]
date-11.5...
! date-11.5 expected: [{2004-02-28 08:00:00}]
! date-11.5 got:      [NULL]
date-11.6...
! date-11.6 expected: [{2004-02-28 07:59:00}]
! date-11.6 got:      [NULL]
date-11.7...
! date-11.7 expected: [{2004-02-28 08:01:00}]
! date-11.7 got:      [NULL]
date-11.8...
! date-11.8 expected: [{2004-02-29 07:59:00}]
! date-11.8 got:      [NULL]
date-11.9...
! date-11.9 expected: [{2004-02-29 08:01:00}]
! date-11.9 got:      [NULL]
date-11.10... Ok
date-12.1... Ok
date-12.2... Ok
date-13.1... Ok
date-13.2... Ok
date-13.3... Ok
date-13.4... Ok
date-13.5... Ok
date-13.6... Ok
date-13.7... Ok
date-13.11... Ok
date-13.12... Ok
date-13.13... Ok
date-13.14... Ok
date-13.15... Ok
date-13.16... Ok
date-13.17... Ok
date-13.18... Ok
date-13.19... Ok
date-13.20... Ok
date-13.21...
! date-13.21 expected: [2454786.5]
! date-13.21 got:      [2454801.5]
date-13.22...
! date-13.22 expected: [2454878.5]
! date-13.22 got:      [2454863.5]
date-13.23...
! date-13.23 expected: [2454284.0]
! date-13.23 got:      [2454466.5]
date-13.24...
! date-13.24 expected: [2455380.0]
! date-13.24 got:      [2455197.5]
date-13.30...
! date-13.30 expected: [2001-07-02]
! date-13.30 got:      [2001-01-01]
date-13.31...
! date-13.31 expected: [2002-07-02]
! date-13.31 got:      [2002-01-01]
date-13.32...
! date-13.32 expected: [2003-07-02]
! date-13.32 got:      [2003-01-01]
date-13.33...
! date-13.33 expected: [2000-07-02]
! date-13.33 got:      [2001-01-01]
date-13.34...
! date-13.34 expected: [1999-07-02]
! date-13.34 got:      [2000-01-01]
date-13.35... Ok
date-13.36...
! date-13.36 expected: [2023-03-01]
! date-13.36 got:      [NULL]
date-13.37...
! date-13.37 expected: [2023-05-01]
! date-13.37 got:      [NULL]
date-14.1...
! date-14.1 expected: [2454629.5]
! date-14.1 got:      [1.1]
date-14.2.0...
! date-14.2.0 expected: [1]
! date-14.2.0 got:      [0]
date-14.2.1...
! date-14.2.1 expected: [1]
! date-14.2.1 got:      [0]
date-14.2.2...
! date-14.2.2 expected: [1]
! date-14.2.2 got:      [0]
date-14.2.3...
! date-14.2.3 expected: [1]
! date-14.2.3 got:      [0]
date-14.2.4...
! date-14.2.4 expected: [1]
! date-14.2.4 got:      [0]
date-14.2.5...
! date-14.2.5 expected: [1]
! date-14.2.5 got:      [0]
date-14.2.6...
! date-14.2.6 expected: [1]
! date-14.2.6 got:      [0]
date-14.2.7...
! date-14.2.7 expected: [1]
! date-14.2.7 got:      [0]
date-14.2.8...
! date-14.2.8 expected: [1]
! date-14.2.8 got:      [0]
date-14.2.9...
! date-14.2.9 expected: [1]
! date-14.2.9 got:      [0]
date-14.2.10...
! date-14.2.10 expected: [1]
! date-14.2.10 got:      [0]
date-14.2.11...
! date-14.2.11 expected: [1]
! date-14.2.11 got:      [0]
date-14.2.12...
! date-14.2.12 expected: [1]
! date-14.2.12 got:      [0]
date-14.2.13...
! date-14.2.13 expected: [1]
! date-14.2.13 got:      [0]
date-14.2.14...
! date-14.2.14 expected: [1]
! date-14.2.14 got:      [0]
date-14.2.15...
! date-14.2.15 expected: [1]
! date-14.2.15 got:      [0]
date-14.2.16...
! date-14.2.16 expected: [1]
! date-14.2.16 got:      [0]
date-14.2.17...
! date-14.2.17 expected: [1]
! date-14.2.17 got:      [0]
date-14.2.18...
! date-14.2.18 expected: [1]
! date-14.2.18 got:      [0]
date-14.2.19...
! date-14.2.19 expected: [1]
! date-14.2.19 got:      [0]
date-14.2.20...
! date-14.2.20 expected: [1]
! date-14.2.20 got:      [0]
date-14.2.21...
! date-14.2.21 expected: [1]
! date-14.2.21 got:      [0]
date-14.2.22...
! date-14.2.22 expected: [1]
! date-14.2.22 got:      [0]
date-14.2.23...
! date-14.2.23 expected: [1]
! date-14.2.23 got:      [0]
date-14.2.24...
! date-14.2.24 expected: [1]
! date-14.2.24 got:      [0]
date-14.2.25...
! date-14.2.25 expected: [1]
! date-14.2.25 got:      [0]
date-14.2.26...
! date-14.2.26 expected: [1]
! date-14.2.26 got:      [0]
date-14.2.27...
! date-14.2.27 expected: [1]
! date-14.2.27 got:      [0]
date-14.2.28...
! date-14.2.28 expected: [1]
! date-14.2.28 got:      [0]
date-14.2.29...
! date-14.2.29 expected: [1]
! date-14.2.29 got:      [0]
date-14.2.30...
! date-14.2.30 expected: [1]
! date-14.2.30 got:      [0]
date-14.2.31...
! date-14.2.31 expected: [1]
! date-14.2.31 got:      [0]
date-14.2.32...
! date-14.2.32 expected: [1]
! date-14.2.32 got:      [0]
date-14.2.33...
! date-14.2.33 expected: [1]
! date-14.2.33 got:      [0]
date-14.2.34...
! date-14.2.34 expected: [1]
! date-14.2.34 got:      [0]
date-14.2.35...
! date-14.2.35 expected: [1]
! date-14.2.35 got:      [0]
date-14.2.36...
! date-14.2.36 expected: [1]
! date-14.2.36 got:      [0]
date-14.2.37...
! date-14.2.37 expected: [1]
! date-14.2.37 got:      [0]
date-14.2.38...
! date-14.2.38 expected: [1]
! date-14.2.38 got:      [0]
date-14.2.39...
! date-14.2.39 expected: [1]
! date-14.2.39 got:      [0]
date-14.2.40...
! date-14.2.40 expected: [1]
! date-14.2.40 got:      [0]
date-14.2.41...
! date-14.2.41 expected: [1]
! date-14.2.41 got:      [0]
date-14.2.42...
! date-14.2.42 expected: [1]
! date-14.2.42 got:      [0]
date-14.2.43...
! date-14.2.43 expected: [1]
! date-14.2.43 got:      [0]
date-14.2.44...
! date-14.2.44 expected: [1]
! date-14.2.44 got:      [0]
date-14.2.45...
! date-14.2.45 expected: [1]
! date-14.2.45 got:      [0]
date-14.2.46...
! date-14.2.46 expected: [1]
! date-14.2.46 got:      [0]
date-14.2.47...
! date-14.2.47 expected: [1]
! date-14.2.47 got:      [0]
date-14.2.48...
! date-14.2.48 expected: [1]
! date-14.2.48 got:      [0]
date-14.2.49...
! date-14.2.49 expected: [1]
! date-14.2.49 got:      [0]
date-14.2.50...
! date-14.2.50 expected: [1]
! date-14.2.50 got:      [0]
date-14.2.51...
! date-14.2.51 expected: [1]
! date-14.2.51 got:      [0]
date-14.2.52...
! date-14.2.52 expected: [1]
! date-14.2.52 got:      [0]
date-14.2.53...
! date-14.2.53 expected: [1]
! date-14.2.53 got:      [0]
date-14.2.54...
! date-14.2.54 expected: [1]
! date-14.2.54 got:      [0]
date-14.2.55...
! date-14.2.55 expected: [1]
! date-14.2.55 got:      [0]
date-14.2.56...
! date-14.2.56 expected: [1]
! date-14.2.56 got:      [0]
date-14.2.57...
! date-14.2.57 expected: [1]
! date-14.2.57 got:      [0]
date-14.2.58...
! date-14.2.58 expected: [1]
! date-14.2.58 got:      [0]
date-14.2.59...
! date-14.2.59 expected: [1]
! date-14.2.59 got:      [0]
date-14.2.60...
! date-14.2.60 expected: [1]
! date-14.2.60 got:      [0]
date-14.2.61...
! date-14.2.61 expected: [1]
! date-14.2.61 got:      [0]
date-14.2.62...
! date-14.2.62 expected: [1]
! date-14.2.62 got:      [0]
date-14.2.63...
! date-14.2.63 expected: [1]
! date-14.2.63 got:      [0]
date-14.2.64...
! date-14.2.64 expected: [1]
! date-14.2.64 got:      [0]
date-14.2.65...
! date-14.2.65 expected: [1]
! date-14.2.65 got:      [0]
date-14.2.66...
! date-14.2.66 expected: [1]
! date-14.2.66 got:      [0]
date-14.2.67...
! date-14.2.67 expected: [1]
! date-14.2.67 got:      [0]
date-14.2.68...
! date-14.2.68 expected: [1]
! date-14.2.68 got:      [0]
date-14.2.69...
! date-14.2.69 expected: [1]
! date-14.2.69 got:      [0]
date-14.2.70...
! date-14.2.70 expected: [1]
! date-14.2.70 got:      [0]
date-14.2.71...
! date-14.2.71 expected: [1]
! date-14.2.71 got:      [0]
date-14.2.72...
! date-14.2.72 expected: [1]
! date-14.2.72 got:      [0]
date-14.2.73...
! date-14.2.73 expected: [1]
! date-14.2.73 got:      [0]
date-14.2.74...
! date-14.2.74 expected: [1]
! date-14.2.74 got:      [0]
date-14.2.75...
! date-14.2.75 expected: [1]
! date-14.2.75 got:      [0]
date-14.2.76...
! date-14.2.76 expected: [1]
! date-14.2.76 got:      [0]
date-14.2.77...
! date-14.2.77 expected: [1]
! date-14.2.77 got:      [0]
date-14.2.78...
! date-14.2.78 expected: [1]
! date-14.2.78 got:      [0]
date-14.2.79...
! date-14.2.79 expected: [1]
! date-14.2.79 got:      [0]
date-14.2.80...
! date-14.2.80 expected: [1]
! date-14.2.80 got:      [0]
date-14.2.81...
! date-14.2.81 expected: [1]
! date-14.2.81 got:      [0]
date-14.2.82...
! date-14.2.82 expected: [1]
! date-14.2.82 got:      [0]
date-14.2.83...
! date-14.2.83 expected: [1]
! date-14.2.83 got:      [0]
date-14.2.84...
! date-14.2.84 expected: [1]
! date-14.2.84 got:      [0]
date-14.2.85...
! date-14.2.85 expected: [1]
! date-14.2.85 got:      [0]
date-14.2.86...
! date-14.2.86 expected: [1]
! date-14.2.86 got:      [0]
date-14.2.87...
! date-14.2.87 expected: [1]
! date-14.2.87 got:      [0]
date-14.2.88...
! date-14.2.88 expected: [1]
! date-14.2.88 got:      [0]
date-14.2.89...
! date-14.2.89 expected: [1]
! date-14.2.89 got:      [0]
date-14.2.90...
! date-14.2.90 expected: [1]
! date-14.2.90 got:      [0]
date-14.2.91...
! date-14.2.91 expected: [1]
! date-14.2.91 got:      [0]
date-14.2.92...
! date-14.2.92 expected: [1]
! date-14.2.92 got:      [0]
date-14.2.93...
! date-14.2.93 expected: [1]
! date-14.2.93 got:      [0]
date-14.2.94...
! date-14.2.94 expected: [1]
! date-14.2.94 got:      [0]
date-14.2.95...
! date-14.2.95 expected: [1]
! date-14.2.95 got:      [0]
date-14.2.96...
! date-14.2.96 expected: [1]
! date-14.2.96 got:      [0]
date-14.2.97...
! date-14.2.97 expected: [1]
! date-14.2.97 got:      [0]
date-14.2.98...
! date-14.2.98 expected: [1]
! date-14.2.98 got:      [0]
date-14.2.99...
! date-14.2.99 expected: [1]
! date-14.2.99 got:      [0]
date-14.2.100...
! date-14.2.100 expected: [1]
! date-14.2.100 got:      [0]
date-14.2.101...
! date-14.2.101 expected: [1]
! date-14.2.101 got:      [0]
date-14.2.102...
! date-14.2.102 expected: [1]
! date-14.2.102 got:      [0]
date-14.2.103...
! date-14.2.103 expected: [1]
! date-14.2.103 got:      [0]
date-14.2.104...
! date-14.2.104 expected: [1]
! date-14.2.104 got:      [0]
date-14.2.105...
! date-14.2.105 expected: [1]
! date-14.2.105 got:      [0]
date-14.2.106...
! date-14.2.106 expected: [1]
! date-14.2.106 got:      [0]
date-14.2.107...
! date-14.2.107 expected: [1]
! date-14.2.107 got:      [0]
date-14.2.108...
! date-14.2.108 expected: [1]
! date-14.2.108 got:      [0]
date-14.2.109...
! date-14.2.109 expected: [1]
! date-14.2.109 got:      [0]
date-14.2.110...
! date-14.2.110 expected: [1]
! date-14.2.110 got:      [0]
date-14.2.111...
! date-14.2.111 expected: [1]
! date-14.2.111 got:      [0]
date-14.2.112...
! date-14.2.112 expected: [1]
! date-14.2.112 got:      [0]
date-14.2.113...
! date-14.2.113 expected: [1]
! date-14.2.113 got:      [0]
date-14.2.114...
! date-14.2.114 expected: [1]
! date-14.2.114 got:      [0]
date-14.2.115...
! date-14.2.115 expected: [1]
! date-14.2.115 got:      [0]
date-14.2.116...
! date-14.2.116 expected: [1]
! date-14.2.116 got:      [0]
date-14.2.117...
! date-14.2.117 expected: [1]
! date-14.2.117 got:      [0]
date-14.2.118...
! date-14.2.118 expected: [1]
! date-14.2.118 got:      [0]
date-14.2.119...
! date-14.2.119 expected: [1]
! date-14.2.119 got:      [0]
date-14.2.120...
! date-14.2.120 expected: [1]
! date-14.2.120 got:      [0]
date-14.2.121...
! date-14.2.121 expected: [1]
! date-14.2.121 got:      [0]
date-14.2.122...
! date-14.2.122 expected: [1]
! date-14.2.122 got:      [0]
date-14.2.123...
! date-14.2.123 expected: [1]
! date-14.2.123 got:      [0]
date-14.2.124...
! date-14.2.124 expected: [1]
! date-14.2.124 got:      [0]
date-14.2.125...
! date-14.2.125 expected: [1]
! date-14.2.125 got:      [0]
date-14.2.126...
! date-14.2.126 expected: [1]
! date-14.2.126 got:      [0]
date-14.2.127...
! date-14.2.127 expected: [1]
! date-14.2.127 got:      [0]
date-14.2.128...
! date-14.2.128 expected: [1]
! date-14.2.128 got:      [0]
date-14.2.129...
! date-14.2.129 expected: [1]
! date-14.2.129 got:      [0]
date-14.2.130...
! date-14.2.130 expected: [1]
! date-14.2.130 got:      [0]
date-14.2.131...
! date-14.2.131 expected: [1]
! date-14.2.131 got:      [0]
date-14.2.132...
! date-14.2.132 expected: [1]
! date-14.2.132 got:      [0]
date-14.2.133...
! date-14.2.133 expected: [1]
! date-14.2.133 got:      [0]
date-14.2.134...
! date-14.2.134 expected: [1]
! date-14.2.134 got:      [0]
date-14.2.135...
! date-14.2.135 expected: [1]
! date-14.2.135 got:      [0]
date-14.2.136...
! date-14.2.136 expected: [1]
! date-14.2.136 got:      [0]
date-14.2.137...
! date-14.2.137 expected: [1]
! date-14.2.137 got:      [0]
date-14.2.138...
! date-14.2.138 expected: [1]
! date-14.2.138 got:      [0]
date-14.2.139...
! date-14.2.139 expected: [1]
! date-14.2.139 got:      [0]
date-14.2.140...
! date-14.2.140 expected: [1]
! date-14.2.140 got:      [0]
date-14.2.141...
! date-14.2.141 expected: [1]
! date-14.2.141 got:      [0]
date-14.2.142...
! date-14.2.142 expected: [1]
! date-14.2.142 got:      [0]
date-14.2.143...
! date-14.2.143 expected: [1]
! date-14.2.143 got:      [0]
date-14.2.144...
! date-14.2.144 expected: [1]
! date-14.2.144 got:      [0]
date-14.2.145...
! date-14.2.145 expected: [1]
! date-14.2.145 got:      [0]
date-14.2.146...
! date-14.2.146 expected: [1]
! date-14.2.146 got:      [0]
date-14.2.147...
! date-14.2.147 expected: [1]
! date-14.2.147 got:      [0]
date-14.2.148...
! date-14.2.148 expected: [1]
! date-14.2.148 got:      [0]
date-14.2.149...
! date-14.2.149 expected: [1]
! date-14.2.149 got:      [0]
date-14.2.150...
! date-14.2.150 expected: [1]
! date-14.2.150 got:      [0]
date-14.2.151...
! date-14.2.151 expected: [1]
! date-14.2.151 got:      [0]
date-14.2.152...
! date-14.2.152 expected: [1]
! date-14.2.152 got:      [0]
date-14.2.153...
! date-14.2.153 expected: [1]
! date-14.2.153 got:      [0]
date-14.2.154...
! date-14.2.154 expected: [1]
! date-14.2.154 got:      [0]
date-14.2.155...
! date-14.2.155 expected: [1]
! date-14.2.155 got:      [0]
date-14.2.156...
! date-14.2.156 expected: [1]
! date-14.2.156 got:      [0]
date-14.2.157...
! date-14.2.157 expected: [1]
! date-14.2.157 got:      [0]
date-14.2.158...
! date-14.2.158 expected: [1]
! date-14.2.158 got:      [0]
date-14.2.159...
! date-14.2.159 expected: [1]
! date-14.2.159 got:      [0]
date-14.2.160...
! date-14.2.160 expected: [1]
! date-14.2.160 got:      [0]
date-14.2.161...
! date-14.2.161 expected: [1]
! date-14.2.161 got:      [0]
date-14.2.162...
! date-14.2.162 expected: [1]
! date-14.2.162 got:      [0]
date-14.2.163...
! date-14.2.163 expected: [1]
! date-14.2.163 got:      [0]
date-14.2.164...
! date-14.2.164 expected: [1]
! date-14.2.164 got:      [0]
date-14.2.165...
! date-14.2.165 expected: [1]
! date-14.2.165 got:      [0]
date-14.2.166...
! date-14.2.166 expected: [1]
! date-14.2.166 got:      [0]
date-14.2.167...
! date-14.2.167 expected: [1]
! date-14.2.167 got:      [0]
date-14.2.168...
! date-14.2.168 expected: [1]
! date-14.2.168 got:      [0]
date-14.2.169...
! date-14.2.169 expected: [1]
! date-14.2.169 got:      [0]
date-14.2.170...
! date-14.2.170 expected: [1]
! date-14.2.170 got:      [0]
date-14.2.171...
! date-14.2.171 expected: [1]
! date-14.2.171 got:      [0]
date-14.2.172...
! date-14.2.172 expected: [1]
! date-14.2.172 got:      [0]
date-14.2.173...
! date-14.2.173 expected: [1]
! date-14.2.173 got:      [0]
date-14.2.174...
! date-14.2.174 expected: [1]
! date-14.2.174 got:      [0]
date-14.2.175...
! date-14.2.175 expected: [1]
! date-14.2.175 got:      [0]
date-14.2.176...
! date-14.2.176 expected: [1]
! date-14.2.176 got:      [0]
date-14.2.177...
! date-14.2.177 expected: [1]
! date-14.2.177 got:      [0]
date-14.2.178...
! date-14.2.178 expected: [1]
! date-14.2.178 got:      [0]
date-14.2.179...
! date-14.2.179 expected: [1]
! date-14.2.179 got:      [0]
date-14.2.180...
! date-14.2.180 expected: [1]
! date-14.2.180 got:      [0]
date-14.2.181...
! date-14.2.181 expected: [1]
! date-14.2.181 got:      [0]
date-14.2.182...
! date-14.2.182 expected: [1]
! date-14.2.182 got:      [0]
date-14.2.183...
! date-14.2.183 expected: [1]
! date-14.2.183 got:      [0]
date-14.2.184...
! date-14.2.184 expected: [1]
! date-14.2.184 got:      [0]
date-14.2.185...
! date-14.2.185 expected: [1]
! date-14.2.185 got:      [0]
date-14.2.186...
! date-14.2.186 expected: [1]
! date-14.2.186 got:      [0]
date-14.2.187...
! date-14.2.187 expected: [1]
! date-14.2.187 got:      [0]
date-14.2.188...
! date-14.2.188 expected: [1]
! date-14.2.188 got:      [0]
date-14.2.189...
! date-14.2.189 expected: [1]
! date-14.2.189 got:      [0]
date-14.2.190...
! date-14.2.190 expected: [1]
! date-14.2.190 got:      [0]
date-14.2.191...
! date-14.2.191 expected: [1]
! date-14.2.191 got:      [0]
date-14.2.192...
! date-14.2.192 expected: [1]
! date-14.2.192 got:      [0]
date-14.2.193...
! date-14.2.193 expected: [1]
! date-14.2.193 got:      [0]
date-14.2.194...
! date-14.2.194 expected: [1]
! date-14.2.194 got:      [0]
date-14.2.195...
! date-14.2.195 expected: [1]
! date-14.2.195 got:      [0]
date-14.2.196...
! date-14.2.196 expected: [1]
! date-14.2.196 got:      [0]
date-14.2.197...
! date-14.2.197 expected: [1]
! date-14.2.197 got:      [0]
date-14.2.198...
! date-14.2.198 expected: [1]
! date-14.2.198 got:      [0]
date-14.2.199...
! date-14.2.199 expected: [1]
! date-14.2.199 got:      [0]
date-14.2.200...
! date-14.2.200 expected: [1]
! date-14.2.200 got:      [0]
date-14.2.201...
! date-14.2.201 expected: [1]
! date-14.2.201 got:      [0]
date-14.2.202...
! date-14.2.202 expected: [1]
! date-14.2.202 got:      [0]
date-14.2.203...
! date-14.2.203 expected: [1]
! date-14.2.203 got:      [0]
date-14.2.204...
! date-14.2.204 expected: [1]
! date-14.2.204 got:      [0]
date-14.2.205...
! date-14.2.205 expected: [1]
! date-14.2.205 got:      [0]
date-14.2.206...
! date-14.2.206 expected: [1]
! date-14.2.206 got:      [0]
date-14.2.207...
! date-14.2.207 expected: [1]
! date-14.2.207 got:      [0]
date-14.2.208...
! date-14.2.208 expected: [1]
! date-14.2.208 got:      [0]
date-14.2.209...
! date-14.2.209 expected: [1]
! date-14.2.209 got:      [0]
date-14.2.210...
! date-14.2.210 expected: [1]
! date-14.2.210 got:      [0]
date-14.2.211...
! date-14.2.211 expected: [1]
! date-14.2.211 got:      [0]
date-14.2.212...
! date-14.2.212 expected: [1]
! date-14.2.212 got:      [0]
date-14.2.213...
! date-14.2.213 expected: [1]
! date-14.2.213 got:      [0]
date-14.2.214...
! date-14.2.214 expected: [1]
! date-14.2.214 got:      [0]
date-14.2.215...
! date-14.2.215 expected: [1]
! date-14.2.215 got:      [0]
date-14.2.216...
! date-14.2.216 expected: [1]
! date-14.2.216 got:      [0]
date-14.2.217...
! date-14.2.217 expected: [1]
! date-14.2.217 got:      [0]
date-14.2.218...
! date-14.2.218 expected: [1]
! date-14.2.218 got:      [0]
date-14.2.219...
! date-14.2.219 expected: [1]
! date-14.2.219 got:      [0]
date-14.2.220...
! date-14.2.220 expected: [1]
! date-14.2.220 got:      [0]
date-14.2.221...
! date-14.2.221 expected: [1]
! date-14.2.221 got:      [0]
date-14.2.222...
! date-14.2.222 expected: [1]
! date-14.2.222 got:      [0]
date-14.2.223...
! date-14.2.223 expected: [1]
! date-14.2.223 got:      [0]
date-14.2.224...
! date-14.2.224 expected: [1]
! date-14.2.224 got:      [0]
date-14.2.225...
! date-14.2.225 expected: [1]
! date-14.2.225 got:      [0]
date-14.2.226...
! date-14.2.226 expected: [1]
! date-14.2.226 got:      [0]
date-14.2.227...
! date-14.2.227 expected: [1]
! date-14.2.227 got:      [0]
date-14.2.228...
! date-14.2.228 expected: [1]
! date-14.2.228 got:      [0]
date-14.2.229...
! date-14.2.229 expected: [1]
! date-14.2.229 got:      [0]
date-14.2.230...
! date-14.2.230 expected: [1]
! date-14.2.230 got:      [0]
date-14.2.231...
! date-14.2.231 expected: [1]
! date-14.2.231 got:      [0]
date-14.2.232...
! date-14.2.232 expected: [1]
! date-14.2.232 got:      [0]
date-14.2.233...
! date-14.2.233 expected: [1]
! date-14.2.233 got:      [0]
date-14.2.234...
! date-14.2.234 expected: [1]
! date-14.2.234 got:      [0]
date-14.2.235...
! date-14.2.235 expected: [1]
! date-14.2.235 got:      [0]
date-14.2.236...
! date-14.2.236 expected: [1]
! date-14.2.236 got:      [0]
date-14.2.237...
! date-14.2.237 expected: [1]
! date-14.2.237 got:      [0]
date-14.2.238...
! date-14.2.238 expected: [1]
! date-14.2.238 got:      [0]
date-14.2.239...
! date-14.2.239 expected: [1]
! date-14.2.239 got:      [0]
date-14.2.240...
! date-14.2.240 expected: [1]
! date-14.2.240 got:      [0]
date-14.2.241...
! date-14.2.241 expected: [1]
! date-14.2.241 got:      [0]
date-14.2.242...
! date-14.2.242 expected: [1]
! date-14.2.242 got:      [0]
date-14.2.243...
! date-14.2.243 expected: [1]
! date-14.2.243 got:      [0]
date-14.2.244...
! date-14.2.244 expected: [1]
! date-14.2.244 got:      [0]
date-14.2.245...
! date-14.2.245 expected: [1]
! date-14.2.245 got:      [0]
date-14.2.246...
! date-14.2.246 expected: [1]
! date-14.2.246 got:      [0]
date-14.2.247...
! date-14.2.247 expected: [1]
! date-14.2.247 got:      [0]
date-14.2.248...
! date-14.2.248 expected: [1]
! date-14.2.248 got:      [0]
date-14.2.249...
! date-14.2.249 expected: [1]
! date-14.2.249 got:      [0]
date-14.2.250...
! date-14.2.250 expected: [1]
! date-14.2.250 got:      [0]
date-14.2.251...
! date-14.2.251 expected: [1]
! date-14.2.251 got:      [0]
date-14.2.252...
! date-14.2.252 expected: [1]
! date-14.2.252 got:      [0]
date-14.2.253...
! date-14.2.253 expected: [1]
! date-14.2.253 got:      [0]
date-14.2.254...
! date-14.2.254 expected: [1]
! date-14.2.254 got:      [0]
date-14.2.255...
! date-14.2.255 expected: [1]
! date-14.2.255 got:      [0]
date-15.1...
! date-15.1 expected: [0.0]
! date-15.1 got:      [0.0000011692754924297333]
date-15.2...
! date-15.2 expected: [1]
! date-15.2 got:      [{}]
date-16.1...
! date-16.1 expected: [NULL]
! date-16.1 got:      [2000-01-01]
date-16.2... Ok
date-16.3... Ok
date-16.4... Ok
date-16.5...
! date-16.5 expected: [5373484.49999999]
! date-16.5 got:      [5373484.499999989]
date-16.6... Ok
date-16.7...
! date-16.7 expected: [NULL]
! date-16.7 got:      [{10000-01-01 00:00:00}]
date-16.8... Ok
date-16.9...
! date-16.9 expected: [NULL]
! date-16.9 got:      [{10000-01-01 00:00:00}]
date-16.10... Ok
date-16.11...
! date-16.11 expected: [NULL]
! date-16.11 got:      [{10000-01-01 00:00:00}]
date-16.12... Ok
date-16.13...
! date-16.13 expected: [NULL]
! date-16.13 got:      [{10000-01-01 12:00:00}]
date-16.14... Ok
date-16.15...
! date-16.15 expected: [NULL]
! date-16.15 got:      [{10000-01-24 12:00:00}]
date-16.16... Ok
date-16.17...
! date-16.17 expected: [NULL]
! date-16.17 got:      [{10000-11-24 12:00:00}]
date-16.20... Ok
date-16.21...
! date-16.21 expected: [NULL]
! date-16.21 got:      [{-4713-11-24 00:00:00}]
date-16.22... Ok
date-16.23...
! date-16.23 expected: [NULL]
! date-16.23 got:      [{-4713-11-24 00:00:00}]
date-16.24... Ok
date-16.25...
! date-16.25 expected: [NULL]
! date-16.25 got:      [{-4713-11-24 00:00:00}]
date-16.26... Ok
date-16.27...
! date-16.27 expected: [NULL]
! date-16.27 got:      [{-4713-11-24 -12:00:00}]
date-16.28...
! date-16.28 expected: [{-4713-12-01 12:00:00}]
! date-16.28 got:      [{-4713-11-30 12:00:00}]
date-16.29...
! date-16.29 expected: [NULL]
! date-16.29 got:      [{-4713-10-31 12:00:00}]
date-16.30... Ok
date-16.31...
! date-16.31 expected: [NULL]
! date-16.31 got:      [{-4714-12-31 12:00:00}]
date-17.1... Ok
date-17.2... Ok
date-17.3... Ok
date-17.4... Ok
date-17.5... Ok
date-17.6...
! date-17.6 expected: [NULL]
! date-17.6 got:      [{-4713-01-01 00:00:00}]
date-17.7... Ok
date-18.1... Ok
date-18.2...
! date-18.2 expected: [0.1]
! date-18.2 got:      [NULL]
date-18.3...
! date-18.3 expected: [0.2]
! date-18.3 got:      [NULL]
date-18.4...
! date-18.4 expected: [0.07001]
! date-18.4 got:      [NULL]
date-18.5...
! date-18.5 expected: [real]
! date-18.5 got:      [null]
date-19.1...
! date-19.1 expected: [2000-01-31]
! date-19.1 got:      [NULL]
date-19.2a...
! date-19.2a expected: [2000-02-29]
! date-19.2a got:      [NULL]
date-19.2b...
! date-19.2b expected: [1999-02-28]
! date-19.2b got:      [NULL]
date-19.2c...
! date-19.2c expected: [1900-02-28]
! date-19.2c got:      [NULL]
date-19.3...
! date-19.3 expected: [2000-03-31]
! date-19.3 got:      [NULL]
date-19.4...
! date-19.4 expected: [2000-04-30]
! date-19.4 got:      [NULL]
date-19.5...
! date-19.5 expected: [2000-05-31]
! date-19.5 got:      [NULL]
date-19.6...
! date-19.6 expected: [2000-06-30]
! date-19.6 got:      [NULL]
date-19.7...
! date-19.7 expected: [2000-07-31]
! date-19.7 got:      [NULL]
date-19.8...
! date-19.8 expected: [2000-08-31]
! date-19.8 got:      [NULL]
date-19.9...
! date-19.9 expected: [2000-09-30]
! date-19.9 got:      [NULL]
date-19.10...
! date-19.10 expected: [2000-10-31]
! date-19.10 got:      [NULL]
date-19.11...
! date-19.11 expected: [2000-11-30]
! date-19.11 got:      [NULL]
date-19.12...
! date-19.12 expected: [2000-12-31]
! date-19.12 got:      [NULL]
date-19.21...
! date-19.21 expected: [2000-01-31]
! date-19.21 got:      [NULL]
date-19.22a...
! date-19.22a expected: [2000-03-02]
! date-19.22a got:      [NULL]
date-19.22b...
! date-19.22b expected: [1999-03-03]
! date-19.22b got:      [NULL]
date-19.22c...
! date-19.22c expected: [1900-03-03]
! date-19.22c got:      [NULL]
date-19.23...
! date-19.23 expected: [2000-03-31]
! date-19.23 got:      [NULL]
date-19.24...
! date-19.24 expected: [2000-05-01]
! date-19.24 got:      [NULL]
date-19.25...
! date-19.25 expected: [2000-05-31]
! date-19.25 got:      [NULL]
date-19.26...
! date-19.26 expected: [2000-07-01]
! date-19.26 got:      [NULL]
date-19.27...
! date-19.27 expected: [2000-07-31]
! date-19.27 got:      [NULL]
date-19.28...
! date-19.28 expected: [2000-08-31]
! date-19.28 got:      [NULL]
date-19.29...
! date-19.29 expected: [2000-10-01]
! date-19.29 got:      [NULL]
date-19.30...
! date-19.30 expected: [2000-10-31]
! date-19.30 got:      [NULL]
date-19.31...
! date-19.31 expected: [2000-12-01]
! date-19.31 got:      [NULL]
date-19.32...
! date-19.32 expected: [2000-12-31]
! date-19.32 got:      [NULL]
date-19.40...
! date-19.40 expected: [2024-03-02]
! date-19.40 got:      [NULL]
date-19.41...
! date-19.41 expected: [2024-02-29]
! date-19.41 got:      [NULL]
date-19.42...
! date-19.42 expected: [2023-03-03]
! date-19.42 got:      [NULL]
date-19.43...
! date-19.43 expected: [2023-02-28]
! date-19.43 got:      [NULL]
date-19.44...
! date-19.44 expected: [2025-03-01]
! date-19.44 got:      [NULL]
date-19.45...
! date-19.45 expected: [2025-02-28]
! date-19.45 got:      [NULL]
date-19.46...
! date-19.46 expected: [1914-03-01]
! date-19.46 got:      [NULL]
date-19.47...
! date-19.47 expected: [1914-02-28]
! date-19.47 got:      [NULL]
date-19.48...
! date-19.48 expected: [1914-02-28]
! date-19.48 got:      [NULL]
date-19.49...
! date-19.49 expected: [1914-03-01]
! date-19.49 got:      [NULL]
date-19.50...
! date-19.50 expected: [2024-02-29]
! date-19.50 got:      [NULL]
date-19.51...
! date-19.51 expected: [2023-02-28]
! date-19.51 got:      [NULL]
date-19.52...
! date-19.52 expected: [2024-03-02]
! date-19.52 got:      [NULL]
date-19.53...
! date-19.53 expected: [2023-03-03]
! date-19.53 got:      [NULL]
date-20.1... Ok
date-20.2... Ok
date-20.3... Ok
date-20.4... Ok
Running "date"

Error in date.test: couldn't read file "date": no such file or directory
couldn't read file "date": no such file or directory
    while executing
"source date"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/date.test" line 683)
    invoked from within
"source $test_file"

==========================================
Test: date
Time: 1s
Status: FAILED
