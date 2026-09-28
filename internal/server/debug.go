package server

import (
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/resp"
)

// debugSpec is NIL.DEBUG, registered only with --enable-debug-commands.
// CLOCK-ADVANCE ms moves the configured config.OffsetClock, which every
// expiry check, version and lease reads, so tests expire keys without
// sleeping.
func (s *Server) debugSpec() command.Spec {
	return command.Spec{
		Name: "nil.debug", Flags: command.Admin, Group: "server",
		Summary: "Test-only hooks, enabled by --enable-debug-commands.",
		Subcommands: []command.Spec{
			{Name: "clock-advance", Arity: 3, Flags: command.Admin,
				Summary: "Moves the server clock forward (or back) by the given milliseconds.",
				Run:     s.cmdClockAdvance},
			{Name: "help", Arity: 2, Summary: "Returns helpful text about the different subcommands.", Run: func(*command.Ctx, [][]byte) resp.Reply {
				return helpReply("NIL.DEBUG",
					"CLOCK-ADVANCE <milliseconds>", "    Move the server clock by <milliseconds>; negative moves it back.")
			}},
		},
	}
}

func (s *Server) cmdClockAdvance(_ *command.Ctx, args [][]byte) resp.Reply {
	ms, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	clock, ok := s.cfg.Clock.(*config.OffsetClock)
	if !ok {
		return resp.Err("ERR the server clock is not adjustable; start with an OffsetClock")
	}
	clock.Advance(time.Duration(ms) * time.Millisecond)
	return resp.OK()
}
