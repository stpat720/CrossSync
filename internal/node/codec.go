package node

import (
	"crosssync/internal/index"
	"crosssync/internal/protocol"
)

// toFileMessage converts an index entry to its wire form.
func toFileMessage(fi *index.FileInfo) protocol.FileMessage {
	return protocol.FileMessage{
		Name:       fi.Name,
		Size:       fi.Size,
		ModifiedS:  fi.ModifiedS,
		ModifiedNs: fi.ModifiedNs,
		Mode:       fi.Mode,
		Type:       int(fi.Type),
		Deleted:    fi.Deleted,
		Invalid:    fi.Invalid,
		Version:    fi.Version,
		BlockSize:  fi.BlockSize,
		Blocks:     fi.Blocks,
	}
}

// fromFileMessage converts a wire form back to an index entry.
func fromFileMessage(fm *protocol.FileMessage) *index.FileInfo {
	return &index.FileInfo{
		Name:       fm.Name,
		Size:       fm.Size,
		ModifiedS:  fm.ModifiedS,
		ModifiedNs: fm.ModifiedNs,
		Mode:       fm.Mode,
		Type:       index.Type(fm.Type),
		Deleted:    fm.Deleted,
		Invalid:    fm.Invalid,
		Version:    fm.Version,
		BlockSize:  fm.BlockSize,
		Blocks:     fm.Blocks,
	}
}
