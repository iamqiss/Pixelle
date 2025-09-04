package models

import (
	"time"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sonet/src/services/drafts_service/proto"
)

// Draft represents a note draft
type Draft struct {
	DraftID              string    `json:"draft_id"`
	UserID               string    `json:"user_id"`
	Title                string    `json:"title"`
	Content              string    `json:"content"`
	ReplyToURI           string    `json:"reply_to_uri"`
	QuoteURI             string    `json:"quote_uri"`
	MentionHandle        string    `json:"mention_handle"`
	Images               []DraftImage `json:"images"`
	Video                *DraftVideo `json:"video"`
	Labels               []string  `json:"labels"`
	Threadgate           []byte    `json:"threadgate"`
	InteractionSettings  []byte    `json:"interaction_settings"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
	IsAutoSaved          bool      `json:"is_auto_saved"`
}

// DraftImage represents an image in a draft
type DraftImage struct {
	URI     string `json:"uri"`
	Width   int32  `json:"width"`
	Height  int32  `json:"height"`
	AltText string `json:"alt_text"`
}

// DraftVideo represents a video in a draft
type DraftVideo struct {
	URI    string `json:"uri"`
	Width  int32  `json:"width"`
	Height int32  `json:"height"`
}

// ToProto converts Draft to protobuf message
func (d *Draft) ToProto() *pb.Draft {
	images := make([]*pb.DraftImage, len(d.Images))
	for i, img := range d.Images {
		images[i] = &pb.DraftImage{
			Uri:     img.URI,
			Width:   img.Width,
			Height:  img.Height,
			AltText: img.AltText,
		}
	}

	var video *pb.DraftVideo
	if d.Video != nil {
		video = &pb.DraftVideo{
			Uri:    d.Video.URI,
			Width:  d.Video.Width,
			Height: d.Video.Height,
		}
	}

	return &pb.Draft{
		DraftId:             d.DraftID,
		UserId:              d.UserID,
		Title:               d.Title,
		Content:             d.Content,
		ReplyToUri:          d.ReplyToURI,
		QuoteUri:            d.QuoteURI,
		MentionHandle:       d.MentionHandle,
		Images:              images,
		Video:               video,
		Labels:              d.Labels,
		Threadgate:          d.Threadgate,
		InteractionSettings: d.InteractionSettings,
		CreatedAt:           timestamppb.New(d.CreatedAt),
		UpdatedAt:           timestamppb.New(d.UpdatedAt),
		IsAutoSaved:         d.IsAutoSaved,
	}
}

// FromProto converts protobuf message to Draft
func (d *Draft) FromProto(proto *pb.Draft) {
	d.DraftID = proto.DraftId
	d.UserID = proto.UserId
	d.Title = proto.Title
	d.Content = proto.Content
	d.ReplyToURI = proto.ReplyToUri
	d.QuoteURI = proto.QuoteUri
	d.MentionHandle = proto.MentionHandle
	d.Labels = proto.Labels
	d.Threadgate = proto.Threadgate
	d.InteractionSettings = proto.InteractionSettings
	d.IsAutoSaved = proto.IsAutoSaved

	if proto.CreatedAt != nil {
		d.CreatedAt = proto.CreatedAt.AsTime()
	}
	if proto.UpdatedAt != nil {
		d.UpdatedAt = proto.UpdatedAt.AsTime()
	}

	// Convert images
	d.Images = make([]DraftImage, len(proto.Images))
	for i, img := range proto.Images {
		d.Images[i] = DraftImage{
			URI:     img.Uri,
			Width:   img.Width,
			Height:  img.Height,
			AltText: img.AltText,
		}
	}

	// Convert video
	if proto.Video != nil {
		d.Video = &DraftVideo{
			URI:    proto.Video.Uri,
			Width:  proto.Video.Width,
			Height: proto.Video.Height,
		}
	}
}