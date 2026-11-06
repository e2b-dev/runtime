package guest

// DefaultName is the guest OS attribute value of the default profile.
const DefaultName = ""

// DefaultLabel is the metric label of the default profile.
const DefaultLabel = "linux"

type defaultProfile struct{}

// Default returns the profile of a template whose guest OS attribute is empty.
func Default() Profile { return defaultProfile{} }

func (defaultProfile) Name() string  { return DefaultName }
func (defaultProfile) Label() string { return DefaultLabel }

func (defaultProfile) Boot() BootConfig                  { return BootConfig{} }
func (defaultProfile) Restore() RestoreConfig            { return RestoreConfig{} }
func (defaultProfile) Firecracker() FirecrackerSelection { return FirecrackerSelection{} }
func (defaultProfile) EnvdInit() EnvdInit                { return EnvdInit{} }
func (defaultProfile) Capabilities() Capabilities        { return Capabilities{} }
func (defaultProfile) Reset() Reset                      { return Reset{} }
func (defaultProfile) Template() TemplateBuild           { return TemplateBuild{} }
func (defaultProfile) FirstBoot() FirstBoot              { return FirstBoot{} }
