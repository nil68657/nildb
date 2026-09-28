package redis

import (
	"strings"
	"testing"
)

// Cases ported from Redis 7.2 tests/unit/geo.tcl, plus the byte-exact
// Sicily transcript from the Redis GEO documentation (GEOPOS prints
// "%.17Lf" digits there).

// coord encodes one coordinate as addReplyHumanLongDouble does.
func (c *conn) coord(s string) string { return c.dbl(s) }

func TestGeoNyc(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(1), "GEOADD", "nyc", "-73.9454966", "40.747533", "lic market")
		c.is(in(0), "GEOADD", "nyc", "-73.9454966", "40.747533", "lic market")
		c.is(in(1), "GEOADD", "nyc", "CH", "40.747533", "-73.9454966", "lic market")
		c.is(in(0), "GEOADD", "nyc", "NX", "-73.9454966", "40.747533", "lic market")
		c.is(in(0), "GEOADD", "nyc", "XX", "-83.9454966", "40.747533", "lic market")
		c.is(in(0), "GEOADD", "nyc", "CH", "NX", "-73.9454966", "40.747533", "lic market")
		c.is(in(1), "GEOADD", "nyc", "CH", "XX", "-73.9454966", "40.747533", "lic market")
		c.is(er("ERR syntax error"), "GEOADD", "nyc", "xx", "nx", "-73.9454966", "40.747533", "lic market")
		c.is(er("ERR syntax error"), "GEOADD", "nyc", "ch", "xx", "foo", "-73.9454966", "40.747533", "lic market")
		c.is(er("ERR value is not a valid float"), "GEOADD", "nyc", "-73.9454966", "40.747533", "lic market", "foo", "bar", "luck market")
		c.is(er("ERR invalid longitude,latitude pair 200.000000,100.000000"), "GEOADD", "nyc", "200", "100", "x")
		c.is(in(6), "GEOADD", "nyc", "-73.9733487", "40.7648057", "central park n/q/r", "-73.9903085", "40.7362513", "union square",
			"-74.0131604", "40.7126674", "wtc one", "-73.7858139", "40.6428986", "jfk", "-73.9375699", "40.7498929", "q4",
			"-73.9564142", "40.7480973", "4545")
		c.is(c.zws("wtc one", "1791873972053020", "union square", "1791875485187452", "central park n/q/r", "1791875761332224",
			"4545", "1791875796750882", "lic market", "1791875804419201", "q4", "1791875830079666", "jfk", "1791895905559723"),
			"ZRANGE", "nyc", "0", "-1", "WITHSCORES")

		c.is(bulks("central park n/q/r", "4545", "union square"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "3", "km", "asc")
		c.is(bulks("central park n/q/r", "4545", "union square"), "GEORADIUS_RO", "nyc", "-73.9798091", "40.7598464", "3", "km", "asc")
		c.is(bulks("central park n/q/r", "4545", "union square", "lic market"), "GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464", "bybox", "6", "6", "km", "asc")
		c.is(er("ERR syntax error"), "GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464", "frommember", "xxx", "bybox", "6", "6", "km", "asc")
		c.is(er("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch"), "geosearch", "nyc", "bybox", "3", "3", "km", "asc", "desc", "withhash", "withdist", "withcoord")
		c.is(er("ERR syntax error"), "GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464", "byradius", "3", "km", "bybox", "3", "3", "km", "asc")
		c.is(er("ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH"), "GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464", "asc", "desc", "withhash", "withdist", "withcoord")
		c.is(er("ERR syntax error"), "GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464", "bybox", "6", "6", "km", "asc", "storedist")
		c.is(ar(bulks("central park n/q/r", "0.7750"), bulks("4545", "2.3651"), bulks("union square", "2.7697")),
			"GEORADIUS", "nyc", "-73.9798091", "40.7598464", "3", "km", "withdist", "asc")
		c.is(ar(bulks("central park n/q/r", "0.7750"), bulks("4545", "2.3651"), bulks("union square", "2.7697"), bulks("lic market", "3.1991")),
			"GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464", "bybox", "6", "6", "km", "withdist", "asc")
		c.is(bulks("central park n/q/r", "4545", "union square"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "3")
		c.is(bulks("wtc one", "union square", "central park n/q/r"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "3", "ANY")
		c.is(bulks("central park n/q/r", "union square", "wtc one"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "3", "ANY", "ASC")
		c.is(er("ERR the ANY argument requires COUNT argument"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "ANY", "ASC")
		c.is(er("ERR syntax error"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT")
		c.is(er("ERR COUNT must be > 0"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "0")
		c.is(bulks("wtc one", "q4"), "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "2", "DESC")
		c.is(bulks("wtc one", "union square", "central park n/q/r", "4545", "lic market"), "GEORADIUSBYMEMBER", "nyc", "wtc one", "7", "km")
		c.is(bulks("wtc one", "union square", "central park n/q/r", "4545", "lic market"), "GEORADIUSBYMEMBER_RO", "nyc", "wtc one", "7", "km")
		c.is(bulks("wtc one", "union square", "central park n/q/r", "4545", "lic market", "q4"), "GEOSEARCH", "nyc", "frommember", "wtc one", "bybox", "14", "14", "km")
		c.is(ar(bulks("wtc one", "0.0000"), bulks("union square", "3.2544"), bulks("central park n/q/r", "6.7000"), bulks("4545", "6.1975"), bulks("lic market", "6.8969")),
			"GEORADIUSBYMEMBER", "nyc", "wtc one", "7", "km", "withdist")
		// geo.tcl matches these coordinates by prefix only.
		got := c.do("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "WITHCOORD", "WITHHASH", "COUNT", "2")
		for _, want := range []string{"*2\r\n*3\r\n" + bs("central park n/q/r") + in(1791875761332224) + "*2\r\n",
			"-73.97334", "40.76480", "*3\r\n" + bs("4545") + in(1791875796750882) + "*2\r\n", "-73.95641", "40.74809"} {
			if !strings.Contains(got, want) {
				t.Errorf("RESP%d WITHCOORD WITHHASH reply %q lacks %q", c.proto, got, want)
			}
		}
		c.hasPrefix("*1\r\n*4\r\n"+bs("central park n/q/r")+bs("0.7750")+in(1791875761332224)+"*2\r\n",
			"GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "WITHHASH", "WITHCOORD", "WITHDIST", "COUNT", "1")
		c.is(er("ERR could not decode requested zset member"), "GEORADIUSBYMEMBER", "nyc", "nomember", "7", "km")
		c.is(er("ERR need numeric radius"), "GEORADIUS", "nyc", "1", "1", "x", "km")
		c.is(er("ERR radius cannot be negative"), "GEORADIUS", "nyc", "1", "1", "-1", "km")
		c.is(er("ERR unsupported unit provided. please use M, KM, FT, MI"), "GEORADIUS", "nyc", "1", "1", "1", "yd")
		c.is(er("ERR need numeric width"), "GEOSEARCH", "nyc", "fromlonlat", "1", "1", "bybox", "a", "1", "km")
		c.is(er("ERR height or width cannot be negative"), "GEOSEARCH", "nyc", "fromlonlat", "1", "1", "bybox", "1", "-1", "km")
	})
}

func TestGeoSicilyTranscript(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(2), "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
		c.is(bs("166274.1516"), "GEODIST", "Sicily", "Palermo", "Catania")
		c.is(bs("166.2742"), "GEODIST", "Sicily", "Palermo", "Catania", "km")
		c.is(bs("103.3182"), "GEODIST", "Sicily", "Palermo", "Catania", "mi")
		c.is(bs("0.0000"), "GEODIST", "Sicily", "Palermo", "Palermo")
		c.is(c.null(), "GEODIST", "Sicily", "Foo", "Bar")
		c.is(c.null(), "GEODIST", "nokey", "Palermo", "Catania")
		c.is(er("ERR syntax error"), "GEODIST", "Sicily", "Palermo", "Catania", "km", "extra")
		c.is(bulks("sqc8b49rny0", "sqdtr74hyu0"), "GEOHASH", "Sicily", "Palermo", "Catania")
		c.is(ar(ar(c.coord("13.36138933897018433"), c.coord("38.11555639549629859")),
			ar(c.coord("15.08726745843887329"), c.coord("37.50266842333162032")), c.nullArr()),
			"GEOPOS", "Sicily", "Palermo", "Catania", "NonExisting")
		c.is(ar(), "GEOPOS", "Sicily")
		c.is(ar(c.nullArr()), "GEOPOS", "nokey", "x")
		c.is(ar(), "GEOHASH", "Sicily")
		c.is(ar(bulks("Palermo", "190.4424"), bulks("Catania", "56.4413")), "GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST")
		c.is(in(2), "GEOADD", "Sicily", "12.758489", "38.788135", "edge1", "17.241510", "38.788135", "edge2")
		c.is(bulks("Catania", "Palermo"), "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
		c.is(ar(
			ar(bs("Catania"), bs("56.4413"), ar(c.coord("15.08726745843887329"), c.coord("37.50266842333162032"))),
			ar(bs("Palermo"), bs("190.4424"), ar(c.coord("13.36138933897018433"), c.coord("38.11555639549629859"))),
			ar(bs("edge2"), bs("279.7403"), ar(c.coord("17.24151045083999634"), c.coord("38.78813451624225195"))),
			ar(bs("edge1"), bs("279.7405"), ar(c.coord("12.7584877610206604"), c.coord("38.78813451624225195")))),
			"GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHCOORD", "WITHDIST")
		c.is(in(1), "GEOADD", "points", "-5.6", "42.6", "test")
		c.is(bulks("ezs42e44yx0"), "GEOHASH", "points", "test")
	})
}

func TestGeoEdges(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "src", "wrong_type")
		for _, args := range [][]string{
			{"GEORADIUS", "src", "1", "1", "1", "km"}, {"GEORADIUS", "src", "1", "1", "1", "km", "store", "dest"},
			{"GEOSEARCH", "src", "fromlonlat", "0", "0", "byradius", "1", "km"},
			{"GEOSEARCHSTORE", "dest", "src", "fromlonlat", "0", "0", "byradius", "1", "km"},
			{"GEORADIUSBYMEMBER", "src", "member", "1", "km"}, {"GEODIST", "src", "member", "1", "km"},
			{"GEOHASH", "src", "member"}, {"GEOPOS", "src", "member"},
		} {
			c.is(er(wrongType), args...)
		}
		c.is(in(1), "DEL", "src")
		c.is(ar(), "GEORADIUS", "src", "1", "1", "1", "km")
		c.is(in(0), "GEORADIUS", "src", "1", "1", "1", "km", "store", "dest")
		c.is(ar(), "GEOSEARCH", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
		c.is(in(0), "GEOSEARCHSTORE", "dest", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
		c.is(ar(), "GEORADIUSBYMEMBER", "src", "member", "1", "km")
		c.is(in(0), "GEORADIUSBYMEMBER", "src", "member", "1", "km", "store", "dest")
		c.is(ar(), "GEOSEARCH", "src", "frommember", "member", "bybox", "1", "1", "km")
		c.is(in(0), "GEOSEARCHSTORE", "dest", "src", "frommember", "member", "bybox", "1", "1", "m")

		c.is(in(2), "GEOADD", "src", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
		c.is(ar(), "GEORADIUS", "src", "1", "1", "1", "km")
		c.is(in(0), "GEORADIUS", "src", "1", "1", "1", "km", "store", "dest")
		c.is(er("ERR could not decode requested zset member"), "GEOSEARCH", "src", "frommember", "member", "bybox", "1", "1", "km")

		c.is(er("ERR syntax error"), "GEORADIUS", "src", "13.361389", "38.115556", "50", "km", "store")
		c.is(er("ERR syntax error"), "GEOSEARCHSTORE", "abc", "src", "fromlonlat", "13.361389", "38.115556", "byradius", "50", "km", "store", "abc")
		c.is(er("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"),
			"GEORADIUS", "src", "13.361389", "38.115556", "50", "km", "store", "points2", "withdist")
		c.is(er("ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"),
			"GEOSEARCHSTORE", "abc", "src", "fromlonlat", "13.361389", "38.115556", "byradius", "50", "km", "withhash")

		c.is(in(2), "GEORADIUS", "src", "13.361389", "38.115556", "500", "km", "store", "points2")
		c.is(bulks("Palermo", "Catania"), "ZRANGE", "points2", "0", "-1")
		c.is(in(2), "GEORADIUSBYMEMBER", "src", "Catania", "500", "km", "storedist", "points2")
		c.is(bulks("Catania", "Palermo"), "ZRANGE", "points2", "0", "-1")
		c.is(c.dbl("0"), "ZSCORE", "points2", "Catania")
		if s := c.do("ZSCORE", "points2", "Palermo"); !strings.Contains(s, "166.27") {
			t.Errorf("RESP%d STOREDIST score of Palermo = %q", c.proto, s)
		}
		c.is(in(2), "GEOSEARCHSTORE", "points3", "src", "fromlonlat", "13.361389", "38.115556", "byradius", "500", "km")
		c.is(bulks("Palermo", "Catania"), "ZRANGE", "points3", "0", "-1")
		c.is(in(1), "GEORADIUS", "src", "13.361389", "38.115556", "500", "km", "storedist", "points4", "desc", "count", "1")
		c.is(bulks("Catania"), "ZRANGE", "points4", "0", "-1")

		c.is(in(1), "GEOADD", "wrap", "179.5", "36", "point1")
		c.is(in(1), "GEOADD", "wrap", "-179.5", "36", "point2")
		c.is(bulks("point1", "point2"), "GEOSEARCH", "wrap", "fromlonlat", "179", "37", "bybox", "400", "400", "km", "asc")
		c.is(bulks("point2", "point1"), "GEOSEARCH", "wrap", "fromlonlat", "-179", "37", "bybox", "400", "400", "km", "asc")
		c.is(in(1), "GEOADD", "small", "-122.407107", "37.794300", "1")
		c.is(in(1), "GEOADD", "small", "-122.227336", "37.794300", "2")
		c.is(ar(bulks("1", "0.0001"), bulks("2", "9.8182")), "GEORADIUS", "small", "-122.407107", "37.794300", "30", "mi", "ASC", "WITHDIST")
		c.is(in(1), "GEOADD", "users", "-47.271613776683807", "-54.534504198047678", "user_000000")
		c.hasPrefix("*1\r\n", "GEORADIUS", "users", "0", "0", "50000", "km", "WITHCOORD")
	})
}

func TestGeoSearchGeometry(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(in(2), "GEOADD", "k1", "-0.15307903289794921875", "85", "n1", "0.3515625", "85.00019260486917005437", "n2")
	c.is(bulks("n1", "n2"), "GEORADIUSBYMEMBER", "k1", "n1", "4891.94", "m")
	c.is(in(2), "ZREM", "k1", "n1", "n2")
	c.is(in(2), "GEOADD", "k1", "-4.95211958885192871094", "85", "n3", "11.25", "85.0511", "n4")
	c.is(bulks("n3", "n4"), "GEORADIUSBYMEMBER", "k1", "n3", "156544", "m")
	c.is(in(2), "ZREM", "k1", "n3", "n4")
	c.is(in(2), "GEOADD", "k1", "-45", "65.50900022111811438208", "n5", "90", "85.0511", "n6")
	c.is(bulks("n5", "n6"), "GEORADIUSBYMEMBER", "k1", "n5", "5009431", "m")
	c.is(in(1), "DEL", "k1")
	c.is(in(2), "GEOADD", "k1", "45", "65", "n1", "-135", "85.05", "n2")
	c.is(bulks("n1", "n2"), "GEORADIUSBYMEMBER", "k1", "n1", "5009431", "m")

	c.is(in(4), "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania",
		"12.758489", "38.788135", "edge1", "17.241510", "38.788135", "eage2")
	c.is(bulks("Catania", "Palermo"), "GEORADIUS", "Sicily", "15", "37", "200", "km", "asc")
	c.is(bulks("Catania", "Palermo", "eage2", "edge1"), "GEOSEARCH", "Sicily", "fromlonlat", "15", "37", "bybox", "400", "400", "km", "asc")

	c.is(in(1), "DEL", "Sicily")
	c.is(in(1), "GEOADD", "Sicily", "12.75", "36.995", "test1")
	c.is(in(1), "GEOADD", "Sicily", "12.75", "36.50", "test2")
	c.is(in(1), "GEOADD", "Sicily", "13.00", "36.50", "test3")
	c.is(bulks("test1"), "GEOSEARCH", "Sicily", "fromlonlat", "15", "37", "bybox", "400", "2", "km")
	c.is(in(0), "GEOADD", "Sicily", "-1", "37.00", "test3")
	c.is(bulks("test1", "test3"), "GEOSEARCH", "Sicily", "fromlonlat", "15", "37", "bybox", "3000", "2", "km", "asc")

	c.is(in(1), "DEL", "Sicily")
	c.is(in(12), "GEOADD", "Sicily", "12.758489", "38.788135", "edge1", "17.241510", "38.788135", "edge2", "17.250000", "35.202000", "edge3",
		"12.750000", "35.202000", "edge4", "12.748489955781654", "37", "edge5", "15", "38.798135872540925", "edge6",
		"17.251510044218346", "37", "edge7", "15", "35.201864127459075", "edge8", "12.692799634687903", "38.798135872540925", "corner1",
		"12.692799634687903", "38.798135872540925", "corner2", "17.200560937451133", "35.201864127459075", "corner3",
		"12.799439062548865", "35.201864127459075", "corner4")
	got := c.do("GEOSEARCH", "Sicily", "fromlonlat", "15", "37", "bybox", "400", "400", "km", "asc")
	for _, want := range []string{"edge1", "edge2", "edge5", "edge7"} {
		if !strings.Contains(got, bs(want)) {
			t.Errorf("corner point test: %s missing from %q", want, got)
		}
	}
	if n := strings.Count(got, "$"); n != 4 {
		t.Errorf("corner point test: %d results in %q", n, got)
	}
}
