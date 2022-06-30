package com.reazon.Schemas

case class SalaryData(
                       rating: Double,
                       companyName: String,
                       jobTitle: String,
                       salary: Int,
                       salariesReported: Int,
                       location: String,
                       employmentStatus: String,
                       jobRoles: String // imp feature
                     )
