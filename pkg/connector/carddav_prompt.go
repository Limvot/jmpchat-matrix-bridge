package connector

import (
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
)

// cardDAVPromptFields runs bridgev2's one-field-per-message prompt flow for fields (same texts as
// bridgev2/commands/login.go: "Please enter your <Name>", description, "Enter `skip` to skip this
// field"), storing commands.CommandState{Action: action}. Password and token fields are redacted with
// ce.Redact() before validation; so is a field named "url" whose answer contains userinfo (a pasted
// "https://user:pass@host"), which its validator then rejects. Invalid input re-prompts the same field.
// When all fields are filled it clears the state and calls done(ce, input).
//
// A near-verbatim copy of mrtc-sip-bridge's hostkit.PromptFields, which cannot be imported in builds
// without the sipcalls tag (hostkit pulls in cgo opus/olm dependencies).
func cardDAVPromptFields(ce *commands.Event, action string, fields []bridgev2.LoginInputDataField,
	done func(ce *commands.Event, input map[string]string)) {
	ps := &cardDAVPromptState{
		action:    action,
		remaining: append([]bridgev2.LoginInputDataField(nil), fields...),
		data:      make(map[string]string, len(fields)),
		done:      done,
	}
	if len(ps.remaining) == 0 {
		commands.StoreCommandState(ce.User, nil)
		done(ce, ps.data)
		return
	}
	ps.promptNext(ce)
}

type cardDAVPromptState struct {
	action    string
	remaining []bridgev2.LoginInputDataField
	data      map[string]string
	done      func(ce *commands.Event, input map[string]string)
}

func (ps *cardDAVPromptState) promptNext(ce *commands.Event) {
	field := ps.remaining[0]
	parts := []string{fmt.Sprintf("Please enter your %s", field.Name)}
	if field.Description != "" {
		parts = append(parts, field.Description)
	}
	if field.Optional {
		parts = append(parts, "Enter `skip` to skip this field")
	}
	ce.Reply("%s", strings.Join(parts, "\n"))
	commands.StoreCommandState(ce.User, &commands.CommandState{
		Next:   commands.MinimalCommandHandlerFunc(ps.submitNext),
		Action: ps.action,
		Meta:   ps,
	})
}

// hasURLUserinfo reports whether a URL-like answer carries "user[:pass]@" before the path.
func hasURLUserinfo(s string) bool {
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return strings.Contains(s, "@")
}

func (ps *cardDAVPromptState) submitNext(ce *commands.Event) {
	field := ps.remaining[0]
	field.FillDefaultValidate()
	if field.Type == bridgev2.LoginInputFieldTypePassword || field.Type == bridgev2.LoginInputFieldTypeToken ||
		(field.ID == "url" && hasURLUserinfo(ce.RawArgs)) {
		ce.Redact()
	}
	val := ce.RawArgs
	if field.Optional && ce.RawArgs == "skip" {
		val = ""
	}
	v, err := field.Validate(val)
	if err != nil {
		ce.Reply("Invalid value: %v", err)
		return
	}
	ps.data[field.ID] = v
	if len(ps.remaining) > 1 {
		ps.remaining = ps.remaining[1:]
		ps.promptNext(ce)
		return
	}
	commands.StoreCommandState(ce.User, nil)
	ps.done(ce, ps.data)
}
