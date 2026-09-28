package redis

import (
	"math/big"
	"slices"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/geohash"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// A GEO set is a sorted set whose scores are 52-bit geohashes
// (internal/geohash, a port of Redis 7.2 geohash.c). GEOSEARCH scans the
// score ranges of the nine cells geohash.Shape.Areas returns, decodes each
// candidate and keeps those Shape.Contains accepts, as geo.c does.

func init() { groups = append(groups, registerGeo) }

func registerGeo(r *command.Registry) {
	const g = "geo"
	w, ro, mr := command.Write, command.ReadOnly, command.MultiRead
	r.Register(
		command.Spec{Name: "geoadd", Arity: -5, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.0",
			Summary: "Adds one or more members to a geospatial index. The key is created if it doesn't exist.", Run: cmdGeoadd},
		command.Spec{Name: "geopos", Arity: -2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.0",
			Summary: "Returns the longitude and latitude of members from a geospatial index.", Run: cmdGeopos},
		command.Spec{Name: "geodist", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.0",
			Summary: "Returns the distance between two members of a geospatial index.", Run: cmdGeodist},
		command.Spec{Name: "geohash", Arity: -2, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.0",
			Summary: "Returns members from a geospatial index as geohash strings.", Run: cmdGeohash},
		command.Spec{Name: "geosearch", Arity: -7, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "6.2.0",
			Summary: "Queries a geospatial index for members inside an area of a box or a circle.", Run: cmdGeosearch},
		command.Spec{Name: "geosearchstore", Arity: -8, Flags: w | mr, FirstKey: 1, LastKey: 2, KeyStep: 1,
			Keys: command.KeysFirstLastStep(1, 1, 1), Group: g, Since: "6.2.0",
			Summary: "Queries a geospatial index for members inside an area of a box or a circle, optionally stores the result.", Run: cmdGeosearchstore},
		command.Spec{Name: "georadius", Arity: -6, Flags: w | mr, FirstKey: 1, LastKey: 1, KeyStep: 1, Keys: geoStoreKeys(6), Group: g, Since: "3.2.0",
			Summary: "Queries a geospatial index for members within a distance from a coordinate, optionally stores the result.", Run: cmdGeoradius},
		command.Spec{Name: "georadiusbymember", Arity: -5, Flags: w | mr, FirstKey: 1, LastKey: 1, KeyStep: 1, Keys: geoStoreKeys(5), Group: g, Since: "3.2.0",
			Summary: "Queries a geospatial index for members within a distance from a member, optionally stores the result.", Run: cmdGeoradiusbymember},
		command.Spec{Name: "georadius_ro", Arity: -6, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.10",
			Summary: "Returns members from a geospatial index that are within a distance from a coordinate.", Run: cmdGeoradiusRO},
		command.Spec{Name: "georadiusbymember_ro", Arity: -5, Flags: ro, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: g, Since: "3.2.10",
			Summary: "Returns members from a geospatial index that are within a distance from a member.", Run: cmdGeoradiusbymemberRO},
	)
}

// geoStoreKeys locks the STORE or STOREDIST target of GEORADIUS and
// GEORADIUSBYMEMBER, found after the base arguments; the source is read
// through the command's snapshot.
func geoStoreKeys(base int) command.KeysFunc {
	return func(kc *command.KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
		var keys []store.LockKey
		for i := base; i+1 < len(args); i++ {
			if equalFold(args[i], "store") || equalFold(args[i], "storedist") {
				keys = append(keys, store.LockKey{Kind: store.LockRedis, NS: uint32(kc.DB), Key: args[i+1]})
				i++
			}
		}
		return keys, nil, nil
	}
}

// parseLonLat is extractLongLatOrReply: both numbers parse first, then
// the pair is checked against the EPSG:900913 limits.
func parseLonLat(lonArg, latArg []byte) (lon, lat float64, rep resp.Reply) {
	lon, ok := resp.ParseFloat(lonArg)
	if !ok {
		return 0, 0, resp.ErrNotFloat
	}
	if lat, ok = resp.ParseFloat(latArg); !ok {
		return 0, 0, resp.ErrNotFloat
	}
	if _, err := geohash.Encode(lon, lat); err != nil {
		return 0, 0, resp.Err("ERR " + err.Error())
	}
	return lon, lat, nil
}

// parseUnit is extractUnitOrReply.
func parseUnit(b []byte) (float64, resp.Reply) {
	u, err := geohash.ParseUnit(string(b))
	if err != nil {
		return 0, resp.Err("ERR " + err.Error())
	}
	return u.ToMeters, nil
}

// parseRadius is extractDistanceOrReply.
func parseRadius(numArg, unitArg []byte) (radius, conv float64, rep resp.Reply) {
	radius, ok := resp.ParseFloat(numArg)
	if !ok {
		return 0, 0, resp.Err("ERR need numeric radius")
	}
	if radius < 0 {
		return 0, 0, resp.Err("ERR radius cannot be negative")
	}
	conv, rep = parseUnit(unitArg)
	return radius, conv, rep
}

// parseBox is extractBoxOrReply.
func parseBox(wArg, hArg, unitArg []byte) (width, height, conv float64, rep resp.Reply) {
	width, ok := resp.ParseFloat(wArg)
	if !ok {
		return 0, 0, 0, resp.Err("ERR need numeric width")
	}
	if height, ok = resp.ParseFloat(hArg); !ok {
		return 0, 0, 0, resp.Err("ERR need numeric height")
	}
	if height < 0 || width < 0 {
		return 0, 0, 0, resp.Err("ERR height or width cannot be negative")
	}
	conv, rep = parseUnit(unitArg)
	return width, height, conv, rep
}

// geoScore turns a sorted-set score back into the geohash it holds.
func geoScore(score float64) geohash.Hash {
	if score < 0 {
		return 0
	}
	return geohash.Hash(uint64(score))
}

// humanDouble writes a coordinate as addReplyHumanLongDouble does: the
// double printed with "%.17Lf" and trailing zeros trimmed, a bulk string
// in RESP2 and a double in RESP3. Redis's documented GEOPOS output
// (13.36138933897018433 for Palermo) is this form.
func humanDouble(w *resp.Writer, x float64) {
	s := formatLongDoubleHuman(new(big.Float).SetPrec(ldPrec).SetFloat64(x))
	if w.Proto() == 3 {
		w.Raw([]byte("," + s + "\r\n"))
		return
	}
	w.Str(s)
}

// cmdGeoadd is geoaddCommand: NX, XX and CH in any order, then triples;
// every coordinate is checked before ZADD runs.
func cmdGeoadd(c *command.Ctx, args [][]byte) resp.Reply {
	var f zaddFlags
	i := 2
scan:
	for ; i < len(args); i++ {
		switch opt := args[i]; {
		case equalFold(opt, "nx"):
			f.nx = true
		case equalFold(opt, "xx"):
			f.xx = true
		case equalFold(opt, "ch"):
			f.ch = true
		default:
			break scan
		}
	}
	n := len(args) - i
	if n%3 != 0 || n == 0 || (f.nx && f.xx) {
		return resp.ErrSyntax
	}
	scores := make([]float64, n/3)
	members := make([][]byte, n/3)
	for j := range scores {
		lon, lat, rep := parseLonLat(args[i+3*j], args[i+3*j+1])
		if rep != nil {
			return rep
		}
		h, _ := geohash.Encode(lon, lat)
		scores[j], members[j] = float64(h), args[i+3*j+2]
	}
	return zaddApply(c, args[1], f, scores, members)
}

// geoLookup reads the geohash of each member; ok[i] is false for an
// absent member or key.
func geoLookup(c *command.Ctx, key []byte, members [][]byte) ([]geohash.Hash, []bool, resp.Reply) {
	k, rep := openColl(c, key, layout.TZSet, false)
	if rep != nil {
		return nil, nil, rep
	}
	hashes := make([]geohash.Hash, len(members))
	ok := make([]bool, len(members))
	if !k.exists {
		return hashes, ok, nil
	}
	cur, err := zscores(c.Reader(), k.m.Version, members)
	if err != nil {
		return nil, nil, storeErr(err)
	}
	for i, s := range cur {
		if s != nil {
			hashes[i], ok[i] = geoScore(*s), true
		}
	}
	return hashes, ok, nil
}

func cmdGeopos(c *command.Ctx, args [][]byte) resp.Reply {
	hashes, ok, rep := geoLookup(c, args[1], args[2:])
	if rep != nil {
		return rep
	}
	return resp.Stream(func(w *resp.Writer) {
		w.ArrayHeader(len(hashes))
		for i, h := range hashes {
			if !ok[i] {
				w.NullArray()
				continue
			}
			lon, lat := h.Decode()
			w.ArrayHeader(2)
			humanDouble(w, lon)
			humanDouble(w, lat)
		}
	})
}

func cmdGeohash(c *command.Ctx, args [][]byte) resp.Reply {
	hashes, ok, rep := geoLookup(c, args[1], args[2:])
	if rep != nil {
		return rep
	}
	out := make([]resp.Reply, len(hashes))
	for i, h := range hashes {
		out[i] = resp.Null()
		if ok[i] {
			out[i] = resp.Str(h.String())
		}
	}
	return resp.Array(out...)
}

// cmdGeodist parses the unit before it reads the key; a missing key or
// member replies a null.
func cmdGeodist(c *command.Ctx, args [][]byte) resp.Reply {
	conv := 1.0
	switch {
	case len(args) == 5:
		var rep resp.Reply
		if conv, rep = parseUnit(args[4]); rep != nil {
			return rep
		}
	case len(args) > 5:
		return resp.ErrSyntax
	}
	hashes, ok, rep := geoLookup(c, args[1], args[2:4])
	switch {
	case rep != nil:
		return rep
	case !ok[0] || !ok[1]:
		return resp.Null()
	}
	lon1, lat1 := hashes[0].Decode()
	lon2, lat2 := hashes[1].Decode()
	return resp.Str(geohash.FormatDistance(geohash.Distance(lon1, lat1, lon2, lat2) / conv))
}

// Search variants of georadiusGeneric.
const (
	geoByCoords    = 1 << iota // GEORADIUS
	geoByMember                // GEORADIUSBYMEMBER
	geoNoStore                 // the _RO variants
	geoSearch                  // GEOSEARCH
	geoSearchStore             // GEOSEARCHSTORE
)

func cmdGeoradius(c *command.Ctx, args [][]byte) resp.Reply {
	return geoRadius(c, args, 1, geoByCoords)
}

func cmdGeoradiusbymember(c *command.Ctx, args [][]byte) resp.Reply {
	return geoRadius(c, args, 1, geoByMember)
}

func cmdGeoradiusRO(c *command.Ctx, args [][]byte) resp.Reply {
	return geoRadius(c, args, 1, geoByCoords|geoNoStore)
}

func cmdGeoradiusbymemberRO(c *command.Ctx, args [][]byte) resp.Reply {
	return geoRadius(c, args, 1, geoByMember|geoNoStore)
}

func cmdGeosearch(c *command.Ctx, args [][]byte) resp.Reply {
	return geoRadius(c, args, 1, geoSearch)
}

func cmdGeosearchstore(c *command.Ctx, args [][]byte) resp.Reply {
	return geoRadius(c, args, 2, geoSearch|geoSearchStore)
}

// geoPoint is one search hit.
type geoPoint struct {
	member   []byte
	lon, lat float64
	dist     float64 // metres until the reply converts it
	hash     geohash.Hash
}

// geoQuery is what georadiusGeneric's parsing produces.
type geoQuery struct {
	storeKey                      []byte
	storeDist                     bool
	lon, lat                      float64
	radius, width, height, conv   float64
	box                           bool
	withDist, withHash, withCoord bool
	anyHit                        bool
	fromMember, fromLonLat        bool
	byRadius, byBox               bool
	sortDir                       int // 0 none, 1 ASC, 2 DESC
	count                         int64
}

// geoRadius is georadiusGeneric. Its checks run in Redis's order: source
// type, the fixed centre and radius of the GEORADIUS forms, the options
// one by one, then the cross-option rules; an absent source is answered
// only after all of them.
func geoRadius(c *command.Ctx, args [][]byte, srcIdx, flags int) resp.Reply {
	k, rep := openColl(c, args[srcIdx], layout.TZSet, false)
	if rep != nil {
		return rep
	}
	var q geoQuery
	base := 2
	switch {
	case flags&geoByCoords != 0:
		base = 6
		if q.lon, q.lat, rep = parseLonLat(args[2], args[3]); rep != nil {
			return rep
		}
		if q.radius, q.conv, rep = parseRadius(args[4], args[5]); rep != nil {
			return rep
		}
	case flags&geoByMember != 0:
		base = 5
		if k.exists {
			if rep = q.center(c, k, args[2]); rep != nil {
				return rep
			}
			if q.radius, q.conv, rep = parseRadius(args[3], args[4]); rep != nil {
				return rep
			}
		}
	case flags&geoSearchStore != 0:
		base = 3
		q.storeKey = args[1]
	}
	if rep = q.parseOptions(c, k, args, base, flags); rep != nil {
		return rep
	}
	if q.storeKey != nil && (q.withDist || q.withHash || q.withCoord) {
		if flags&geoSearchStore != 0 {
			return resp.Err("ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
		}
		return resp.Err("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
	}
	if flags&geoSearch != 0 && !q.fromMember && !q.fromLonLat {
		return resp.Err("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for " + string(args[0]))
	}
	if flags&geoSearch != 0 && !q.byRadius && !q.byBox {
		return resp.Err("ERR exactly one of BYRADIUS and BYBOX can be specified for " + string(args[0]))
	}
	if q.anyHit && q.count == 0 {
		return resp.Err("ERR the ANY argument requires COUNT argument")
	}
	if !k.exists {
		if q.storeKey != nil {
			return storeZset(c, q.storeKey, nil)
		}
		return resp.Array()
	}
	if q.count != 0 && q.sortDir == 0 && !q.anyHit {
		q.sortDir = 1
	}
	return q.run(c, k)
}

// center sets the search centre to a member's position.
func (q *geoQuery) center(c *command.Ctx, k *coll, member []byte) resp.Reply {
	cur, err := zscores(c.Reader(), k.m.Version, [][]byte{member})
	if err != nil {
		return storeErr(err)
	}
	if cur[0] == nil {
		return resp.Err("ERR could not decode requested zset member")
	}
	q.lon, q.lat = geoScore(*cur[0]).Decode()
	return nil
}

// parseOptions reads the options after the base arguments, one by one
// as georadiusGeneric's loop does, replying the first error.
func (q *geoQuery) parseOptions(c *command.Ctx, k *coll, args [][]byte, base, flags int) resp.Reply {
	var rep resp.Reply
	for i := base; i < len(args); i++ {
		arg, left := args[i], len(args)-i-1
		switch {
		case equalFold(arg, "withdist"):
			q.withDist = true
		case equalFold(arg, "withhash"):
			q.withHash = true
		case equalFold(arg, "withcoord"):
			q.withCoord = true
		case equalFold(arg, "any"):
			q.anyHit = true
		case equalFold(arg, "asc"):
			q.sortDir = 1
		case equalFold(arg, "desc"):
			q.sortDir = 2
		case equalFold(arg, "count") && left >= 1:
			n, ok := resp.ParseInt(args[i+1])
			if !ok {
				return resp.ErrNotInteger
			}
			if n <= 0 {
				return resp.Err("ERR COUNT must be > 0")
			}
			q.count = n
			i++
		case (equalFold(arg, "store") || equalFold(arg, "storedist")) && left >= 1 &&
			flags&geoNoStore == 0 && flags&geoSearch == 0:
			q.storeKey, q.storeDist = args[i+1], equalFold(arg, "storedist")
			i++
		case equalFold(arg, "storedist") && flags&geoSearchStore != 0:
			q.storeDist = true
		case equalFold(arg, "frommember") && left >= 1 && flags&geoSearch != 0 && !q.fromLonLat:
			if k.exists {
				if rep = q.center(c, k, args[i+1]); rep != nil {
					return rep
				}
			}
			q.fromMember = true
			i++
		case equalFold(arg, "fromlonlat") && left >= 2 && flags&geoSearch != 0 && !q.fromMember:
			if q.lon, q.lat, rep = parseLonLat(args[i+1], args[i+2]); rep != nil {
				return rep
			}
			q.fromLonLat = true
			i += 2
		case equalFold(arg, "byradius") && left >= 2 && flags&geoSearch != 0 && !q.byBox:
			if q.radius, q.conv, rep = parseRadius(args[i+1], args[i+2]); rep != nil {
				return rep
			}
			q.byRadius, q.box = true, false
			i += 2
		case equalFold(arg, "bybox") && left >= 3 && flags&geoSearch != 0 && !q.byRadius:
			if q.width, q.height, q.conv, rep = parseBox(args[i+1], args[i+2], args[i+3]); rep != nil {
				return rep
			}
			q.byBox, q.box = true, true
			i += 3
		default:
			return resp.ErrSyntax
		}
	}
	return nil
}

// run searches sorted set k and replies, or stores the hits.
func (q *geoQuery) run(c *command.Ctx, k *coll) resp.Reply {
	shape := geohash.Shape{Lon: q.lon, Lat: q.lat}
	if q.box {
		shape.Width, shape.Height = q.width*q.conv, q.height*q.conv
	} else {
		shape.Radius = q.radius * q.conv
	}
	var limit int64
	if q.anyHit {
		limit = q.count
	}
	points, err := geoCollect(c.Reader(), k.m.Version, shape, limit)
	if err != nil {
		return storeErr(err)
	}
	if len(points) == 0 && q.storeKey == nil {
		return resp.Array()
	}
	switch q.sortDir {
	case 1:
		slices.SortStableFunc(points, func(a, b geoPoint) int { return cmpDist(a.dist, b.dist) })
	case 2:
		slices.SortStableFunc(points, func(a, b geoPoint) int { return cmpDist(b.dist, a.dist) })
	}
	if q.count > 0 && int64(len(points)) > q.count {
		points = points[:q.count]
	}
	for i := range points {
		points[i].dist /= q.conv
	}
	if q.storeKey != nil {
		es := make([]zentry, len(points))
		for i, p := range points {
			es[i] = zentry{member: p.member, score: float64(p.hash)}
			if q.storeDist {
				es[i].score = p.dist
			}
		}
		return storeZset(c, q.storeKey, es)
	}
	opts := 0
	for _, on := range []bool{q.withDist, q.withHash, q.withCoord} {
		if on {
			opts++
		}
	}
	withDist, withHash, withCoord := q.withDist, q.withHash, q.withCoord
	return resp.Stream(func(w *resp.Writer) {
		w.ArrayHeader(len(points))
		for _, p := range points {
			if opts > 0 {
				w.ArrayHeader(opts + 1)
			}
			w.Bulk(p.member)
			if withDist {
				w.Str(geohash.FormatDistance(p.dist))
			}
			if withHash {
				w.Int(int64(p.hash))
			}
			if withCoord {
				w.ArrayHeader(2)
				humanDouble(w, p.lon)
				humanDouble(w, p.lat)
			}
		}
	})
}

func cmpDist(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// geoCollect is membersOfAllNeighbors: it scans the score range [Min,
// Max) of each search cell in order and keeps the members inside the
// shape, stopping once limit hits are found (COUNT ... ANY) when limit
// is set.
func geoCollect(r store.Reader, ver uint64, shape geohash.Shape, limit int64) ([]geoPoint, error) {
	var out []geoPoint
	for _, area := range shape.Areas() {
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
		lo := scoreFloor(ver, float64(area.Min), false)
		hi := scoreFloor(ver, float64(area.Max), false)
		it := r.Iter(store.CFZScore, lo, hi, store.IterOpts{FillCache: true})
		for it.SeekToFirst(); it.Valid(); it.Next() {
			_, score, member := layout.SplitScoreKey(it.Key())
			h := geoScore(score)
			lon, lat := h.Decode()
			if dist, in := shape.Contains(lon, lat); in {
				out = append(out, geoPoint{member: clone(member), lon: lon, lat: lat, dist: dist, hash: h})
				if limit > 0 && int64(len(out)) >= limit {
					break
				}
			}
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
