/*
 * Copyright 2021 Arman Bilge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vancats.data

import org.scalacheck.Arbitrary
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.scalacheck.Parameters

class TsilSpec extends Specification with ScalaCheck {

  implicit val params: Parameters = Parameters(maxSize = 10)

  implicit def arbitraryTsil[A: Arbitrary]: Arbitrary[Tsil[A]] =
    Arbitrary(Arbitrary.arbitrary[List[A]].map(Tsil.fromList))

  implicit class TsilOps[A](t: Tsil[A]) {
    def safeToList: List[A] = {
      require(t.size == t.toList.size)
      t.toList
    }
  }

  "Tsil" should {

    "round-trip with list" in prop { (l: List[Int]) =>
      Tsil.fromList(l).safeToList should_=== l
    }

    "uncons" in prop { (t: Tsil[Int]) =>
      t.uncons.map {
        case (head, tail) =>
          (head, tail.safeToList)
      } should_=== Some(t.safeToList).collect { case head :: tail => (head, tail) }
    }

    "initLast" in prop { (t: Tsil[Int]) =>
      t.initLast.map {
        case (init, last) =>
          (init.safeToList, last)
      } should_=== Some(t.safeToList).filter(_.nonEmpty).map(l => (l.init, l.last))
    }

    "headOption" in prop { (t: Tsil[Int]) => t.headOption should_=== t.safeToList.headOption }

    "lastOption" in prop { (t: Tsil[Int]) => t.lastOption should_=== t.safeToList.lastOption }

    "apply" in prop { (t: Tsil[Int]) =>
      forall(0 until t.size) { i => t(i) should_=== t.safeToList(i) }
    }

    "isDefinedAt" in prop { (t: Tsil[Int], i: Int) =>
      t.isDefinedAt(i) should_=== t.safeToList.isDefinedAt(i)
    }

    "isEmpty" in prop { (t: Tsil[Int]) => t.isEmpty should_=== t.safeToList.isEmpty }

    "nonEmpty" in prop { (t: Tsil[Int]) => t.nonEmpty should_=== t.safeToList.nonEmpty }

    "indexOf" in prop { (t: Tsil[Int], i: Int) =>
      forall(i :: t.toList) { x => t.indexOf(x) should_=== t.safeToList.indexOf(x) }
    }

    "lastIndexOf" in prop { (t: Tsil[Int], i: Int) =>
      forall(i :: t.toList) { x => t.lastIndexOf(x) should_=== t.safeToList.lastIndexOf(x) }
    }

    "concat" in prop { (x: Tsil[Int], y: Tsil[Int]) =>
      (x ++ y).safeToList should_=== x.safeToList ++ y.safeToList
    }

    "prepend" in prop { (x: Int, y: Tsil[Int]) =>
      (x +: y).safeToList should_=== x +: y.safeToList
    }

    "append" in prop { (x: Tsil[Int], y: Int) =>
      (x :+ y).safeToList should_=== x.safeToList :+ y
    }

    "take" in prop { (t: Tsil[Int]) =>
      forall(-t.size to 2 * t.size) { n =>
        t.take(n).safeToList should_=== t.safeToList.take(n)
      }
    }

    "takeRight" in prop { (t: Tsil[Int]) =>
      forall(-t.size to 2 * t.size) { n =>
        t.takeRight(n).safeToList should_=== t.safeToList.takeRight(n)
      }
    }

    "drop" in prop { (t: Tsil[Int]) =>
      forall(-t.size to 2 * t.size) { n =>
        t.drop(n).safeToList should_=== t.safeToList.drop(n)
      }
    }

    "dropRight" in prop { (t: Tsil[Int]) =>
      forall(-t.size to 2 * t.size) { n =>
        t.dropRight(n).safeToList should_=== t.safeToList.dropRight(n)
      }
    }

    "slice" in prop { (t: Tsil[Int]) =>
      forall(for {
        i <- -t.size to 2 * t.size
        j <- -t.size to 2 * t.size
      } yield (i, j)) {
        case (from, until) =>
          t.slice(from, until).safeToList should_=== t.safeToList.slice(from, until)
      }
    }

    "splitAt" in prop { (t: Tsil[Int]) =>
      forall(-t.size to 2 * t.size) { n =>
        (t.splitAt(n) match {
          case (x, y) => (x.safeToList, y.safeToList)
        }) should_=== t.safeToList.splitAt(n)
      }
    }

  }

}
