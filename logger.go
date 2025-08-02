package reservo

// noopLogger - implements Logger interface with no-op methods
type noopLogger struct{}

func (n *noopLogger) Debugf(format string, args ...interface{}) {}
func (n *noopLogger) Infof(format string, args ...interface{})  {}
func (n *noopLogger) Warnf(format string, args ...interface{})  {}
func (n *noopLogger) Errorf(format string, args ...interface{}) {}

// formatLogMsg adds RESERVO prefix to log messages
func formatLogMsg(format string) string {
	return "RESERVO: " + format
}
