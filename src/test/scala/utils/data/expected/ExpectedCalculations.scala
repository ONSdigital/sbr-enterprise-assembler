package utils.data.expected

import model.{AdminData, Employment, Region}

//import model.Calculations

/**
  *
  */
trait ExpectedCalculations {

  val expectedAdminDataCalculations: Seq[AdminData] = Seq(

    AdminData("3000000011", Some("17"), Some("20"), Some("585"), None, None, None, Some("585")),
    AdminData("2000000011", Some("2"), Some("4"), None, None, Some("390"), None, Some("390")),
    AdminData("4000000011", Some("5"), Some("8"), None, Some("500"), Some("260"), Some("1000"), Some("760")),
    AdminData("111111111-TEST-ERN", Some("3"), Some("5"), Some("85"), None, None, None, Some("85")),
    AdminData("5000000011", Some("5"), Some("5"), None, Some("500"), None, Some("1000"), Some("500"))

  )

  val expectedRegionCalculations: Seq[Region] = Seq(

    Region("3000000011", "E12000006"),
    Region("2000000011", "E12000009"),
    Region("4000000011", "E12000008"),
    Region("111111111-TEST-ERN", "E12000007"),
    Region("5000000011", "W99999999")

  )

  val expectedEmploymentCalculations: Seq[Employment] = Seq(

    Employment("3000000011", "17"),
    Employment("2000000011", "4"),
    Employment("4000000011", "7"),
    Employment("111111111-TEST-ERN", "3"),
    Employment("5000000011", "5")

  )
}
